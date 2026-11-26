// Pas de console en release sous Windows
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{BufferSize, Device, SampleFormat, Stream, StreamConfig};
use eframe::egui;
use ringbuf::{
    HeapRb,
    traits::{Consumer, Observer, Producer, Split},
};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const BUFFER_CHOICES: [u32; 5] = [32, 64, 128, 256, 512];
const DEFAULT_BUFFER_IDX: usize = 2; // 128 images
const SCRATCH_FRAMES: usize = 256;
const RING_SAMPLES: usize = 1 << 16;

// ---------------------------------------------------------------------------
// État partagé entre threads audio et interface (uniquement des atomiques :
// aucun verrou dans les callbacks audio)
// ---------------------------------------------------------------------------
struct Shared {
    gain: AtomicU32,     // f32 en bits
    peak_in: AtomicU32,  // f32 >= 0 en bits (l'ordre des bits = l'ordre numérique)
    peak_out: AtomicU32,
    underruns: AtomicU32,
    overruns: AtomicU32,
    stream_error: AtomicBool,
}

impl Shared {
    fn new() -> Self {
        Self {
            gain: AtomicU32::new(1.0f32.to_bits()),
            peak_in: AtomicU32::new(0),
            peak_out: AtomicU32::new(0),
            underruns: AtomicU32::new(0),
            overruns: AtomicU32::new(0),
            stream_error: AtomicBool::new(false),
        }
    }
}

/// Copie une image (frame) d'entrée vers le nombre de canaux de sortie.
/// Gain 1.0 = copie exacte des échantillons (aucune perte de qualité).
#[inline(always)]
fn map_frame(src: &[f32], dst: &mut [f32], g: f32) {
    let (i, o) = (src.len(), dst.len());
    if i == o {
        for k in 0..o {
            dst[k] = (src[k] * g).clamp(-1.0, 1.0);
        }
    } else if i == 1 {
        let v = (src[0] * g).clamp(-1.0, 1.0);
        dst.fill(v);
    } else if o == 1 {
        let s: f32 = src.iter().sum::<f32>() / i as f32;
        dst[0] = (s * g).clamp(-1.0, 1.0);
    } else {
        for k in 0..o {
            dst[k] = if k < i { (src[k] * g).clamp(-1.0, 1.0) } else { 0.0 };
        }
    }
}

fn dev_name(d: &Device) -> String {
    d.description()
        .map(|desc| desc.name().to_string())
        .unwrap_or_else(|_| "Périphérique inconnu".into())
}

// ---------------------------------------------------------------------------
// Réglages mémorisés (fichier texte simple, sans dépendance)
// ---------------------------------------------------------------------------
#[derive(Clone, PartialEq)]
struct Settings {
    input: String,
    output: String,
    buffer: u32,
    gain: f32,
    autostart: bool,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            input: String::new(),
            output: String::new(),
            buffer: BUFFER_CHOICES[DEFAULT_BUFFER_IDX],
            gain: 1.0,
            autostart: false,
        }
    }
}

fn settings_path() -> PathBuf {
    if let Some(a) = std::env::var_os("APPDATA") {
        return PathBuf::from(a).join("AudioRouter").join("settings.ini");
    }
    std::env::current_exe()
        .ok()
        .and_then(|p| p.parent().map(|d| d.join("settings.ini")))
        .unwrap_or_else(|| PathBuf::from("settings.ini"))
}

impl Settings {
    fn load() -> Self {
        let mut s = Self::default();
        if let Ok(text) = std::fs::read_to_string(settings_path()) {
            for line in text.lines() {
                let Some((k, v)) = line.split_once('=') else { continue };
                let v = v.trim();
                match k.trim() {
                    "input" => s.input = v.to_string(),
                    "output" => s.output = v.to_string(),
                    "buffer" => {
                        if let Ok(b) = v.parse::<u32>() {
                            s.buffer = b;
                        }
                    }
                    "gain" => {
                        if let Ok(g) = v.parse::<f32>() {
                            s.gain = g.clamp(0.0, 2.0);
                        }
                    }
                    "autostart" => s.autostart = v == "true",
                    _ => {}
                }
            }
        }
        s
    }

    fn save(&self) {
        let path = settings_path();
        if let Some(dir) = path.parent() {
            let _ = std::fs::create_dir_all(dir);
        }
        let text = format!(
            "input={}\noutput={}\nbuffer={}\ngain={}\nautostart={}\n",
            self.input, self.output, self.buffer, self.gain, self.autostart
        );
        let _ = std::fs::write(path, text);
    }
}

// ---------------------------------------------------------------------------
// Instance unique (Windows) : verrou de fichier tenu tant que l'app tourne,
// libéré automatiquement à la fermeture (même en cas de crash)
// ---------------------------------------------------------------------------
const WINDOW_TITLE: &str = "Audio Router";

/// Ok(verrou) = première instance ; Err(()) = une instance tourne déjà.
#[cfg(windows)]
fn single_instance() -> Result<Option<std::fs::File>, ()> {
    use std::os::windows::fs::OpenOptionsExt;
    let mut path = settings_path();
    path.set_file_name("instance.lock");
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    match std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .share_mode(0) // accès exclusif
        .open(&path)
    {
        Ok(f) => Ok(Some(f)),
        Err(e) if e.raw_os_error() == Some(32) => Err(()), // ERROR_SHARING_VIOLATION
        Err(_) => Ok(None), // autre erreur : on ne bloque pas l'utilisateur
    }
}

#[cfg(not(windows))]
fn single_instance() -> Result<Option<std::fs::File>, ()> {
    Ok(None)
}

/// Ramène la fenêtre déjà ouverte au premier plan.
#[cfg(windows)]
fn focus_existing_window() {
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        FindWindowW, IsIconic, SW_RESTORE, SetForegroundWindow, ShowWindow,
    };
    let title: Vec<u16> = WINDOW_TITLE.encode_utf16().chain(std::iter::once(0)).collect();
    unsafe {
        let hwnd = FindWindowW(std::ptr::null(), title.as_ptr());
        if !hwnd.is_null() {
            if IsIconic(hwnd) != 0 {
                ShowWindow(hwnd, SW_RESTORE);
            }
            SetForegroundWindow(hwnd);
        }
    }
}

#[cfg(not(windows))]
fn focus_existing_window() {}

// ---------------------------------------------------------------------------
// Moteur audio
// ---------------------------------------------------------------------------
struct Engine {
    _input: Stream,
    _output: Stream,
    info: String,
}

fn build_streams(
    in_dev: &Device,
    out_dev: &Device,
    buffer: BufferSize,
    shared: &Arc<Shared>,
) -> Result<(Stream, Stream, StreamConfig, StreamConfig), String> {
    let in_def = in_dev
        .default_input_config()
        .map_err(|e| format!("Entrée : {e}"))?;
    let out_def = out_dev
        .default_output_config()
        .map_err(|e| format!("Sortie : {e}"))?;

    if in_def.sample_format() != SampleFormat::F32 || out_def.sample_format() != SampleFormat::F32 {
        return Err("Format non pris en charge (f32 attendu). Utilise le format par défaut de Windows.".into());
    }

    let mut in_cfg: StreamConfig = in_def.config();
    let mut out_cfg: StreamConfig = out_def.config();
    // Même fréquence des deux côtés : aucun rééchantillonnage = qualité intacte
    out_cfg.sample_rate = in_cfg.sample_rate;
    in_cfg.buffer_size = buffer;
    out_cfg.buffer_size = buffer;

    let in_ch = in_cfg.channels as usize;
    let out_ch = out_cfg.channels as usize;

    let (mut prod, mut cons) = HeapRb::<f32>::new(RING_SAMPLES).split();

    // --- Callback d'entrée : applique le gain, adapte les canaux, pousse dans le ring
    let sh_in = shared.clone();
    let mut scratch = vec![0.0f32; SCRATCH_FRAMES * out_ch];
    let input = in_dev
        .build_input_stream(
            in_cfg.clone(),
            move |data: &[f32], _| {
                let g = f32::from_bits(sh_in.gain.load(Relaxed));
                // Niveau d'entrée : une seule passe, vectorisable
                let peak_in = data.iter().fold(0.0f32, |m, &s| m.max(s.abs()));

                if in_ch == out_ch && g == 1.0 {
                    // Chemin rapide : copie directe, identique bit à bit (aucun calcul)
                    let free = prod.vacant_len() / out_ch * out_ch;
                    let take = data.len().min(free);
                    prod.push_slice(&data[..take]);
                    if take < data.len() {
                        sh_in.overruns.fetch_add(1, Relaxed);
                    }
                } else {
                    for chunk in data.chunks(in_ch * SCRATCH_FRAMES) {
                        let mut n = 0;
                        for src in chunk.chunks_exact(in_ch) {
                            map_frame(src, &mut scratch[n..n + out_ch], g);
                            n += out_ch;
                        }
                        // On ne pousse que des images entières (jamais de décalage de canaux)
                        let free = prod.vacant_len() / out_ch * out_ch;
                        let take = n.min(free);
                        prod.push_slice(&scratch[..take]);
                        if take < n {
                            sh_in.overruns.fetch_add(1, Relaxed);
                        }
                    }
                }
                sh_in.peak_in.fetch_max(peak_in.to_bits(), Relaxed);
            },
            {
                let sh = shared.clone();
                move |_e| sh.stream_error.store(true, Relaxed)
            },
            None,
        )
        .map_err(|e| format!("Entrée : {e}"))?;

    // --- Callback de sortie : lit le ring, rattrape la dérive d'horloge
    let sh_out = shared.clone();
    let mut primed = false;
    let output = out_dev
        .build_output_stream(
            out_cfg.clone(),
            move |data: &mut [f32], _| {
                let n = data.len();
                let avail = cons.occupied_len();

                // Pré-remplissage : on attend d'avoir un buffer complet d'avance
                if !primed {
                    if avail >= n {
                        primed = true;
                    } else {
                        data.fill(0.0);
                        return;
                    }
                }

                // Trop d'avance accumulée (dérive d'horloge) : on rattrape pour garder la latence minimale
                let max_fill = n * 4;
                if avail > max_fill {
                    let drop = (avail - n) / out_ch * out_ch;
                    cons.skip(drop);
                    sh_out.overruns.fetch_add(1, Relaxed);
                }

                let got = cons.pop_slice(data);
                if got < n {
                    data[got..].fill(0.0);
                    sh_out.underruns.fetch_add(1, Relaxed);
                    primed = false;
                }

                let mut peak = 0.0f32;
                for &s in data.iter() {
                    peak = peak.max(s.abs());
                }
                sh_out.peak_out.fetch_max(peak.to_bits(), Relaxed);
            },
            {
                let sh = shared.clone();
                move |_e| sh.stream_error.store(true, Relaxed)
            },
            None,
        )
        .map_err(|e| {
            format!("Sortie : {e}\nAstuce : mets les deux périphériques à la même fréquence dans les paramètres son de Windows.")
        })?;

    Ok((input, output, in_cfg, out_cfg))
}

fn start_engine(
    in_dev: &Device,
    out_dev: &Device,
    buffer_frames: u32,
    shared: &Arc<Shared>,
) -> Result<Engine, String> {
    shared.underruns.store(0, Relaxed);
    shared.overruns.store(0, Relaxed);
    shared.stream_error.store(false, Relaxed);

    // 1) buffer fixe demandé ; 2) repli sur le buffer par défaut du pilote
    let (input, output, in_cfg, _out_cfg, fixed) =
        match build_streams(in_dev, out_dev, BufferSize::Fixed(buffer_frames), shared) {
            Ok((i, o, ic, oc)) => (i, o, ic, oc, true),
            Err(_) => {
                let (i, o, ic, oc) = build_streams(in_dev, out_dev, BufferSize::Default, shared)?;
                (i, o, ic, oc, false)
            }
        };

    input.play().map_err(|e| e.to_string())?;
    output.play().map_err(|e| e.to_string())?;

    let sr = in_cfg.sample_rate as f32;
    let info = if fixed {
        // ≈ buffer entrée + 1 buffer d'avance + buffer sortie
        let ms = buffer_frames as f32 * 3.0 / sr * 1000.0;
        format!("{} Hz · buffer {} · latence ≈ {:.1} ms", in_cfg.sample_rate, buffer_frames, ms)
    } else {
        format!("{} Hz · buffer par défaut du pilote (le buffer fixe n'est pas accepté)", in_cfg.sample_rate)
    };

    Ok(Engine { _input: input, _output: output, info })
}

// ---------------------------------------------------------------------------
// Interface
// ---------------------------------------------------------------------------
struct App {
    inputs: Vec<Device>,
    outputs: Vec<Device>,
    in_names: Vec<String>,
    out_names: Vec<String>,
    in_idx: usize,
    out_idx: usize,
    buffer_idx: usize,
    gain: f32,
    engine: Option<Engine>,
    shared: Arc<Shared>,
    status: String,
    meter_in: f32,
    meter_out: f32,
    autostart: bool,
    saved: Settings,
    dirty_since: Option<Instant>,
}

impl App {
    fn new() -> Self {
        let st = Settings::load();
        let mut app = Self {
            inputs: vec![],
            outputs: vec![],
            in_names: vec![],
            out_names: vec![],
            in_idx: 0,
            out_idx: 0,
            buffer_idx: BUFFER_CHOICES
                .iter()
                .position(|&b| b == st.buffer)
                .unwrap_or(DEFAULT_BUFFER_IDX),
            gain: st.gain,
            engine: None,
            shared: Arc::new(Shared::new()),
            status: String::new(),
            meter_in: 0.0,
            meter_out: 0.0,
            autostart: st.autostart,
            saved: st.clone(),
            dirty_since: None,
        };
        app.shared.gain.store(app.gain.to_bits(), Relaxed);
        app.refresh_devices(Some(st.input), Some(st.output));
        if app.autostart {
            app.start();
        }
        app
    }

    fn current_settings(&self) -> Settings {
        Settings {
            input: self.in_names.get(self.in_idx).cloned().unwrap_or_default(),
            output: self.out_names.get(self.out_idx).cloned().unwrap_or_default(),
            buffer: BUFFER_CHOICES[self.buffer_idx],
            gain: self.gain,
            autostart: self.autostart,
        }
    }

    fn refresh_devices(&mut self, in_pref: Option<String>, out_pref: Option<String>) {
        let host = cpal::default_host();
        self.inputs = host.input_devices().map(|d| d.collect()).unwrap_or_default();
        self.outputs = host.output_devices().map(|d| d.collect()).unwrap_or_default();
        self.in_names = self.inputs.iter().map(dev_name).collect();
        self.out_names = self.outputs.iter().map(dev_name).collect();

        let def_in = host.default_input_device().map(|d| dev_name(&d));
        let def_out = host.default_output_device().map(|d| dev_name(&d));
        // Priorité : périphérique mémorisé, puis périphérique par défaut de Windows
        self.in_idx = in_pref
            .and_then(|n| self.in_names.iter().position(|x| *x == n))
            .or_else(|| def_in.and_then(|n| self.in_names.iter().position(|x| *x == n)))
            .unwrap_or(0);
        self.out_idx = out_pref
            .and_then(|n| self.out_names.iter().position(|x| *x == n))
            .or_else(|| def_out.and_then(|n| self.out_names.iter().position(|x| *x == n)))
            .unwrap_or(0);
    }

    fn start(&mut self) {
        self.engine = None;
        let (Some(i), Some(o)) = (self.inputs.get(self.in_idx), self.outputs.get(self.out_idx)) else {
            self.status = "Aucun périphérique sélectionné".into();
            return;
        };
        match start_engine(i, o, BUFFER_CHOICES[self.buffer_idx], &self.shared) {
            Ok(e) => {
                self.status = e.info.clone();
                self.engine = Some(e);
            }
            Err(e) => self.status = e,
        }
    }
}

fn meter(ui: &mut egui::Ui, label: &str, v: f32) {
    ui.horizontal(|ui| {
        ui.label(label);
        ui.add(egui::ProgressBar::new(v.clamp(0.0, 1.0)).desired_width(260.0));
    });
}

impl eframe::App for App {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        // Niveaux : montée instantanée, descente douce
        let pi = f32::from_bits(self.shared.peak_in.swap(0, Relaxed));
        let po = f32::from_bits(self.shared.peak_out.swap(0, Relaxed));
        self.meter_in = pi.max(self.meter_in * 0.85);
        self.meter_out = po.max(self.meter_out * 0.85);

        if self.engine.is_some() && self.shared.stream_error.load(Relaxed) {
            self.engine = None;
            self.status = "Erreur audio : périphérique déconnecté ou occupé. Relance.".into();
        }

        egui::CentralPanel::default().show(ui, |ui| {
            ui.heading("Audio Router");
            ui.add_space(6.0);

            let running = self.engine.is_some();

            ui.add_enabled_ui(!running, |ui| {
                egui::ComboBox::from_label("Entrée")
                    .width(280.0)
                    .selected_text(self.in_names.get(self.in_idx).cloned().unwrap_or_default())
                    .show_ui(ui, |ui| {
                        for (i, n) in self.in_names.iter().enumerate() {
                            ui.selectable_value(&mut self.in_idx, i, n);
                        }
                    });
                egui::ComboBox::from_label("Sortie")
                    .width(280.0)
                    .selected_text(self.out_names.get(self.out_idx).cloned().unwrap_or_default())
                    .show_ui(ui, |ui| {
                        for (i, n) in self.out_names.iter().enumerate() {
                            ui.selectable_value(&mut self.out_idx, i, n);
                        }
                    });
                egui::ComboBox::from_label("Buffer (images)")
                    .selected_text(BUFFER_CHOICES[self.buffer_idx].to_string())
                    .show_ui(ui, |ui| {
                        for (i, b) in BUFFER_CHOICES.iter().enumerate() {
                            ui.selectable_value(&mut self.buffer_idx, i, b.to_string());
                        }
                    });
                if ui.button("Rafraîchir les périphériques").clicked() {
                    let ip = self.in_names.get(self.in_idx).cloned();
                    let op = self.out_names.get(self.out_idx).cloned();
                    self.refresh_devices(ip, op);
                }
            });

            ui.add_space(6.0);
            if ui
                .add(egui::Slider::new(&mut self.gain, 0.0..=2.0).text("Gain (1.0 = intact)"))
                .changed()
            {
                self.shared.gain.store(self.gain.to_bits(), Relaxed);
            }

            ui.checkbox(&mut self.autostart, "Démarrer automatiquement au lancement");

            ui.add_space(6.0);
            let label = if running { "Arrêter" } else { "Démarrer" };
            if ui.add_sized([140.0, 32.0], egui::Button::new(label)).clicked() {
                if running {
                    self.engine = None;
                    self.status = "Arrêté".into();
                } else {
                    self.start();
                }
            }

            ui.add_space(8.0);
            meter(ui, "Entrée", self.meter_in);
            meter(ui, "Sortie", self.meter_out);

            ui.add_space(6.0);
            ui.label(&self.status);
            if running {
                ui.small(format!(
                    "Sous-alimentations : {} · Rattrapages : {}",
                    self.shared.underruns.load(Relaxed),
                    self.shared.overruns.load(Relaxed)
                ));
                ui.small("Si ça craque : augmente le buffer d'un cran.");
            }
            ui.small("Réglages mémorisés automatiquement (%APPDATA%\\AudioRouter).");
        });

        // Sauvegarde différée : écrit le fichier 0,4 s après le dernier changement
        let cur = self.current_settings();
        if cur != self.saved {
            let t = *self.dirty_since.get_or_insert_with(Instant::now);
            if t.elapsed() >= Duration::from_millis(400) {
                cur.save();
                self.saved = cur;
                self.dirty_since = None;
            } else {
                ctx.request_repaint_after(Duration::from_millis(200));
            }
        } else {
            self.dirty_since = None;
        }

        if self.engine.is_some() {
            let minimized = ctx.input(|i| i.viewport().minimized.unwrap_or(false));
            let ms = if minimized { 500 } else { 50 };
            ctx.request_repaint_after(Duration::from_millis(ms));
        }
    }
}

fn main() -> eframe::Result {
    let _instance_lock = match single_instance() {
        Ok(lock) => lock,
        Err(()) => {
            focus_existing_window();
            return Ok(());
        }
    };

    let icon = egui::IconData {
        rgba: include_bytes!("../assets/icon_64.rgba").to_vec(),
        width: 64,
        height: 64,
    };
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([440.0, 360.0])
            .with_min_inner_size([420.0, 340.0])
            .with_icon(icon),
        ..Default::default()
    };
    eframe::run_native(
        WINDOW_TITLE,
        options,
        Box::new(|_cc| Ok(Box::new(App::new()))),
    )
}
