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

    #[test]
    fn test_is_workspace_file() {
        assert!(is_workspace_file("application/vnd.google-apps.document"));
        assert!(is_workspace_file("application/vnd.google-apps.spreadsheet"));
        assert!(!is_workspace_file("application/pdf"));
        assert!(!is_workspace_file("image/png"));
    }

    #[test]
    fn test_generate_html_redirect_document() {
        let entry = generate_desktop_entry("ABC123", "Mi Documento", "application/vnd.google-apps.document");
        
        assert!(entry.contains("<!DOCTYPE html>"));
        assert!(entry.contains("<meta http-equiv=\"refresh\""));
        assert!(entry.contains("https://docs.google.com/document/d/ABC123/edit"));
        assert!(entry.contains("<title>Mi Documento</title>"));
    }

    #[test]
    fn test_generate_html_redirect_spreadsheet() {
        let entry = generate_desktop_entry("XYZ789", "Hoja de Cálculo", "application/vnd.google-apps.spreadsheet");
        
        assert!(entry.contains("https://docs.google.com/spreadsheets/d/XYZ789/edit"));
    }
}
