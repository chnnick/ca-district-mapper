use std::sync::Mutex;

use tauri::{Manager, RunEvent, WebviewUrl, WebviewWindowBuilder};
use tauri_plugin_shell::process::{CommandChild, CommandEvent};
use tauri_plugin_shell::ShellExt;

/// Holds the spawned Python sidecar so we can kill it on exit.
struct SidecarState(Mutex<Option<CommandChild>>);

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .manage(SidecarState(Mutex::new(None)))
        .setup(|app| {
            let app_data_dir = app.path().app_data_dir()?;
            std::fs::create_dir_all(&app_data_dir)?;

            let sidecar = app
                .shell()
                .sidecar("district-mapper-backend")?
                .env("APP_DATA_DIR", app_data_dir.to_string_lossy().to_string());

            let (mut rx, child) = sidecar.spawn()?;
            app.state::<SidecarState>()
                .0
                .lock()
                .expect("SidecarState mutex poisoned")
                .replace(child);

            let app_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let mut window_built = false;
                while let Some(event) = rx.recv().await {
                    match event {
                        CommandEvent::Stdout(bytes) => {
                            let line = String::from_utf8_lossy(&bytes);
                            let trimmed = line.trim();
                            if !window_built {
                                if let Some(port) = parse_port(trimmed) {
                                    if let Err(err) = open_main_window(&app_handle, port) {
                                        eprintln!(
                                            "[district-mapper] failed to open window: {err}"
                                        );
                                    } else {
                                        window_built = true;
                                        eprintln!(
                                            "[district-mapper] sidecar listening on \
                                             http://127.0.0.1:{port}"
                                        );
                                    }
                                }
                            }
                        }
                        CommandEvent::Stderr(bytes) => {
                            eprint!("[sidecar] {}", String::from_utf8_lossy(&bytes));
                        }
                        CommandEvent::Error(msg) => {
                            eprintln!("[sidecar:error] {msg}");
                        }
                        CommandEvent::Terminated(payload) => {
                            eprintln!("[sidecar:terminated] {payload:?}");
                        }
                        _ => {}
                    }
                }
            });

            Ok(())
        })
        .build(tauri::generate_context!())
        .expect("error while building tauri application")
        .run(|app_handle, event| {
            if let RunEvent::ExitRequested { .. } = event {
                if let Some(state) = app_handle.try_state::<SidecarState>() {
                    if let Some(child) = state
                        .0
                        .lock()
                        .expect("SidecarState mutex poisoned")
                        .take()
                    {
                        let _ = child.kill();
                    }
                }
            }
        });
}

fn parse_port(line: &str) -> Option<u16> {
    let value: serde_json::Value = serde_json::from_str(line).ok()?;
    let port = value.get("port")?.as_u64()?;
    u16::try_from(port).ok()
}

fn open_main_window(app: &tauri::AppHandle, port: u16) -> tauri::Result<()> {
    let api_base = format!("http://127.0.0.1:{port}");
    let init_script = format!("window.__API_BASE__ = {};", json_string(&api_base));

    WebviewWindowBuilder::new(app, "main", WebviewUrl::App("index.html".into()))
        .title("California District Mapper")
        .inner_size(1280.0, 800.0)
        .min_inner_size(800.0, 600.0)
        .initialization_script(&init_script)
        .build()?;
    Ok(())
}

fn json_string(s: &str) -> String {
    serde_json::to_string(s).expect("JSON string serialization is infallible")
}
