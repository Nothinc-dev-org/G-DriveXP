/// Utilidades para generar accesos directos .desktop para archivos de Google Workspace

/// Genera el contenido de un archivo .desktop para un documento de Google Workspace
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

    // Determinar el icono apropiado según el tipo
    let icon = match mime_type {
        "application/vnd.google-apps.document" => "x-office-document",
        "application/vnd.google-apps.spreadsheet" => "x-office-spreadsheet",
        "application/vnd.google-apps.presentation" => "x-office-presentation",
        "application/vnd.google-apps.form" => "text-html",
        "application/vnd.google-apps.drawing" => "image-x-generic",
        _ => "text-html",
    };

    // Formato estándar FreeDesktop.org Desktop Entry
    format!(
        "[Desktop Entry]\n\
         Version=1.0\n\
         Type=Link\n\
         Name={}\n\
         Icon={}\n\
         URL={}\n",
        name, icon, url
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
    fn test_generate_desktop_entry_document() {
        let entry = generate_desktop_entry("ABC123", "Mi Documento", "application/vnd.google-apps.document");
        
        assert!(entry.contains("[Desktop Entry]"));
        assert!(entry.contains("Type=Link"));
        assert!(entry.contains("Name=Mi Documento"));
        assert!(entry.contains("https://docs.google.com/document/d/ABC123/edit"));
        assert!(entry.contains("Icon=x-office-document"));
    }

    #[test]
    fn test_generate_desktop_entry_spreadsheet() {
        let entry = generate_desktop_entry("XYZ789", "Hoja de Cálculo", "application/vnd.google-apps.spreadsheet");
        
        assert!(entry.contains("https://docs.google.com/spreadsheets/d/XYZ789/edit"));
        assert!(entry.contains("Icon=x-office-spreadsheet"));
    }
}
