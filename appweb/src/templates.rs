use crate::params::RoomParameters;
use minijinja::{AutoEscape, Environment, context};
use std::error::Error;
use std::fs;
use std::path::Path;

/// The two parsed page templates (`index_template.html`, `full_template.html`).
///
/// Unlike the Go port — which rewrote the AppRTC Jinja2 constructs into Go
/// `html/template` syntax with a `strings.Replacer` (`jinjaToGo`) because Go has
/// no Jinja engine — `minijinja` speaks Jinja2 directly, so the original
/// templates are parsed verbatim (`{% if %}`, `{{ x }}`, `{{ x | safe }}`) and
/// the rewriting layer is gone.
pub struct Templates {
    env: Environment<'static>,
}

impl Templates {
    /// Read and parse the templates from `<web_root>/html`.
    pub fn load(web_root: &str) -> Result<Self, Box<dyn Error + Send + Sync>> {
        let dir = Path::new(web_root).join("html");
        let index = fs::read_to_string(dir.join("index_template.html"))?;
        let full = fs::read_to_string(dir.join("full_template.html"))?;
        Ok(Self::from_sources(index, full)?)
    }

    /// Parse the templates from in-memory sources.
    ///
    /// Auto-escaping is left **off** and `context_for` pre-escapes the plain
    /// fields itself (see there). minijinja's built-in HTML escaper is more
    /// aggressive than the Jinja2 these templates were written for — it escapes
    /// `/` to `&#x2f;`, which breaks the URLs sitting inside `<script>` JS-string
    /// literals (`&#x2f;` is not decoded in raw-text `<script>` content). Escaping
    /// exactly like Jinja2/markupsafe instead keeps those URLs intact.
    fn from_sources(index: String, full: String) -> Result<Self, minijinja::Error> {
        let mut env = Environment::new();
        env.set_auto_escape_callback(|_| AutoEscape::None);
        env.add_template_owned("index", index)?;
        env.add_template_owned("full", full)?;
        Ok(Self { env })
    }

    /// Render `index_template.html`.
    pub fn render_index(&self, params: &RoomParameters) -> Result<String, minijinja::Error> {
        self.env.get_template("index")?.render(context_for(params))
    }

    /// Render `full_template.html`.
    pub fn render_full(&self, params: &RoomParameters) -> Result<String, minijinja::Error> {
        self.env.get_template("full")?.render(context_for(params))
    }
}

/// Escape a plain string the way Jinja2/markupsafe does (`& < > " '`, but not
/// `/`). Auto-escaping is off, so the plain fields are escaped here; this matches
/// the environment the AppRTC templates were authored for, and is safe in both
/// the HTML and the `<script>` JS-string contexts they appear in (a stray quote
/// becomes `&#34;`/`&#39;`, which neither breaks out of an attribute nor out of a
/// JS string literal).
fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&#34;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// Build the render context, the port of `roomParameters.toTemplateContext`.
///
/// Plain string fields are pre-escaped (see [`escape`]); the JSON/JS-blob and
/// raw-HTML fields are passed verbatim (Go typed them `template.JS`/
/// `template.HTML`). `error_messages`/`warning_messages` are re-marshaled from
/// their `Vec<String>` to a JS array literal, exactly as `toTemplateContext`
/// does with `mustJSON`.
fn context_for(p: &RoomParameters) -> minijinja::Value {
    let error_messages = serde_json::to_string(&p.error_messages).unwrap_or_else(|_| "null".into());
    let warning_messages =
        serde_json::to_string(&p.warning_messages).unwrap_or_else(|_| "null".into());

    context! {
        // Plain string params, pre-escaped.
        room_id => escape(&p.room_id),
        room_link => escape(&p.room_link),
        header_message => escape(&p.header_message),
        wss_url => escape(&p.wss_url),
        wss_post_url => escape(&p.wss_post_url),
        ice_server_url => escape(&p.ice_server_url),
        ice_server_transports => escape(&p.ice_server_transports),

        // JSON-valued params injected raw (template.JS in Go).
        error_messages => error_messages,
        warning_messages => warning_messages,
        is_loopback => &p.is_loopback,
        media_constraints => &p.media_constraints,
        offer_options => &p.offer_options,
        pc_config => &p.pc_config,
        pc_constraints => &p.pc_constraints,
        bypass_join_confirmation => &p.bypass_join_confirmation,
        version_info => &p.version_info,

        // Raw HTML injected verbatim (template.HTML in Go).
        include_loopback_js => &p.include_loopback_js,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_plain_fields_and_injects_safe_fields_raw() {
        // Exercises the escaping contract with a special char (`<`) in one plain
        // field and one pre-marked-safe field: the plain field must be
        // HTML-escaped, the safe field injected verbatim (this also proves
        // `context!` preserves the safe-string marker rather than stripping it).
        let src = "id={{ room_id }} \
                   url='{{ wss_url }}' \
                   cfg={{ pc_config | safe }} \
                   loop={{ is_loopback }}\
                   {% if room_id %} has-room{% endif %}"
            .to_string();
        let t = Templates::from_sources(src.clone(), src).unwrap();

        let params = RoomParameters {
            room_id: "a<b".to_string(),
            wss_url: "ws://host:9000/ws".to_string(),
            pc_config: r#"{"x":1<2}"#.to_string(),
            is_loopback: "1<2".to_string(),
            ..Default::default()
        };
        let out = t.render_index(&params).unwrap();

        assert!(out.contains("id=a&lt;b"), "plain field not escaped: {out}");
        // The slashes in a URL inside a JS string must survive verbatim (the bug
        // minijinja's built-in `/`-escaping would introduce).
        assert!(
            out.contains("url='ws://host:9000/ws'"),
            "url slashes mangled: {out}"
        );
        assert!(
            out.contains(r#"cfg={"x":1<2}"#),
            "| safe blob altered: {out}"
        );
        assert!(out.contains("loop=1<2"), "safe field not raw: {out}");
        assert!(out.contains("has-room"), "{{% if %}} not handled: {out}");
    }

    #[test]
    fn empty_message_lists_render_as_js_arrays() {
        let src = "e={{ error_messages }} w={{ warning_messages }}".to_string();
        let t = Templates::from_sources(src.clone(), src).unwrap();
        let out = t.render_index(&RoomParameters::default()).unwrap();
        assert_eq!(out, "e=[] w=[]", "message lists not marshaled to JS arrays");
    }

    #[test]
    fn renders_full_template_and_serializes_message_lists() {
        let t = Templates::from_sources(
            "index".to_string(),
            "{{ room_id }}|{{ error_messages }}|{{ warning_messages }}|{{ include_loopback_js }}"
                .to_string(),
        )
        .unwrap();
        let params = RoomParameters {
            room_id: "room".into(),
            client_id: "client".into(),
            error_messages: vec!["bad offer".into()],
            warning_messages: vec!["slow network".into()],
            include_loopback_js: "<script src=\"loopback.js\"></script>".into(),
            ..Default::default()
        };
        let out = t.render_full(&params).unwrap();
        assert!(out.contains("room|[\"bad offer\"]|[\"slow network\"]"));
        assert!(out.contains("<script src=\"loopback.js\"></script>"));
    }

    #[test]
    fn escapes_all_plain_string_delimiters_without_escaping_slashes() {
        let t = Templates::from_sources("{{ value }}".into(), "full".into()).unwrap();
        let params = RoomParameters {
            room_id: "&<>'\"/".into(),
            ..Default::default()
        };
        let out = t.render_index(&params).unwrap();
        assert_eq!(out, "");

        let t = Templates::from_sources("{{ room_id }}".into(), "full".into()).unwrap();
        let out = t.render_index(&params).unwrap();
        assert_eq!(out, "&amp;&lt;&gt;&#39;&#34;/");
    }

    #[test]
    fn load_reads_index_and_full_templates_from_web_root() {
        let root = std::env::temp_dir().join(format!("appweb-templates-{}", rand::random::<u64>()));
        std::fs::create_dir_all(root.join("html")).unwrap();
        std::fs::write(root.join("html/index_template.html"), "index").unwrap();
        std::fs::write(root.join("html/full_template.html"), "full").unwrap();
        let templates = Templates::load(root.to_str().unwrap()).unwrap();
        assert_eq!(
            templates.render_index(&RoomParameters::default()).unwrap(),
            "index"
        );
        assert_eq!(
            templates.render_full(&RoomParameters::default()).unwrap(),
            "full"
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn load_returns_error_when_template_is_missing() {
        let root = std::env::temp_dir().join(format!("appweb-missing-{}", rand::random::<u64>()));
        std::fs::create_dir_all(root.join("html")).unwrap();
        std::fs::write(root.join("html/index_template.html"), "index").unwrap();
        assert!(Templates::load(root.to_str().unwrap()).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
