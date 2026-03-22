/// Utilidades para generar accesos directos HTML para archivos de Google Workspace
/// NOTA: Usamos HTML con meta-refresh en lugar de .desktop porque Nautilus 3.30+
/// no ejecuta archivos .desktop desde montajes FUSE por políticas de seguridad.

/// Genera el contenido de un archivo HTML redirector para un documento de Google Workspace
pub fn generate_desktop_entry(file_id: &str, name: &str, mime_type: &str) -> String {
    // Determinar la URL base según el tipo de documento
    let url = match mime_type {
        "application/vnd.google-apps.document" => {
            format!("https://docs.google.com/document/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.spreadsheet" => {
            format!("https://docs.google.com/spreadsheets/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.presentation" => {
            format!("https://docs.google.com/presentation/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.form" => {
            format!("https://docs.google.com/forms/d/{}/edit", file_id)
        }
        "application/vnd.google-apps.drawing" => {
            format!("https://docs.google.com/drawings/d/{}/edit", file_id)
        }
        _ => {
            // Fallback: abrir en Drive
            format!("https://drive.google.com/file/d/{}/view", file_id)
        }
    };

    // Archivo HTML con meta-refresh que abre el navegador automáticamente
    format!(
        r#"<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="0; url={}">
<title>{}</title>
</head>
<body>
<p>Abriendo <a href="{}">{}</a>...</p>
<script>window.location.href="{}";</script>
</body>
</html>
"#,
        url, name, url, name, url
    )
}

/// Detecta si un MIME type es de Google Workspace
pub fn is_workspace_file(mime_type: &str) -> bool {
    mime_type.starts_with("application/vnd.google-apps.")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::*;

    #[rstest]
    #[case::document("application/vnd.google-apps.document", true)]
    #[case::spreadsheet("application/vnd.google-apps.spreadsheet", true)]
    #[case::presentation("application/vnd.google-apps.presentation", true)]
    #[case::form("application/vnd.google-apps.form", true)]
    #[case::drawing("application/vnd.google-apps.drawing", true)]
    #[case::pdf("application/pdf", false)]
    #[case::png("image/png", false)]
    #[case::plain_text("text/plain", false)]
    #[case::empty("", false)]
    fn test_is_workspace_file(#[case] mime: &str, #[case] expected: bool) {
        assert_eq!(is_workspace_file(mime), expected);
    }

    #[rstest]
    #[case::document(
        "application/vnd.google-apps.document",
        "https://docs.google.com/document/d/ID123/edit"
    )]
    #[case::spreadsheet(
        "application/vnd.google-apps.spreadsheet",
        "https://docs.google.com/spreadsheets/d/ID123/edit"
    )]
    #[case::presentation(
        "application/vnd.google-apps.presentation",
        "https://docs.google.com/presentation/d/ID123/edit"
    )]
    #[case::form(
        "application/vnd.google-apps.form",
        "https://docs.google.com/forms/d/ID123/edit"
    )]
    #[case::drawing(
        "application/vnd.google-apps.drawing",
        "https://docs.google.com/drawings/d/ID123/edit"
    )]
    #[case::fallback(
        "application/vnd.google-apps.unknown_type",
        "https://drive.google.com/file/d/ID123/view"
    )]
    fn test_generate_desktop_entry_urls(#[case] mime: &str, #[case] expected_url: &str) {
        let entry = generate_desktop_entry("ID123", "Test", mime);
        assert!(entry.contains(expected_url), "Expected URL '{}' in:\n{}", expected_url, entry);
    }

    #[rstest]
    fn test_html_structure() {
        let entry = generate_desktop_entry("ABC", "Mi Doc", "application/vnd.google-apps.document");
        assert!(entry.contains("<!DOCTYPE html>"));
        assert!(entry.contains("<meta charset=\"UTF-8\">"));
        assert!(entry.contains("<meta http-equiv=\"refresh\""));
        assert!(entry.contains("<title>Mi Doc</title>"));
        assert!(entry.contains("window.location.href="));
    }

    #[rstest]
    fn test_url_appears_three_times() {
        let entry = generate_desktop_entry("X", "N", "application/vnd.google-apps.document");
        let url = "https://docs.google.com/document/d/X/edit";
        let count = entry.matches(url).count();
        assert_eq!(count, 3, "URL should appear in meta-refresh, href, and JS redirect");
    }
}
