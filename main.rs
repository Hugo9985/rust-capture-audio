#![windows_subsystem = "windows"]

use std::f64::consts::PI;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::{FromSample, SampleFormat, SizedSample};
use eframe::egui;
use ringbuf::{
    HeapCons, HeapProd, HeapRb,
    traits::{Consumer, Observer, Producer, Split},
};

/// Trame stéréo : le ring buffer ne peut jamais désaligner gauche et droite.
type Frame = [f32; 2];

// ───────────────────────── État partagé (atomiques) ─────────────────────────

#[derive(Default)]
struct Shared {
    gain: AtomicU32,       // f32 (bits)
    latency_ms: AtomicU32, // cible demandée
    peak_l: AtomicU32,     // f32 positif (bits) : l'ordre des bits suit l'ordre des valeurs
    peak_r: AtomicU32,
    in_chunk: AtomicU32,  // plus gros bloc de capture observé (trames)
    buffer_us: AtomicU32, // remplissage lissé du ring, en µs
    dropouts: AtomicU32,
    fatal: AtomicBool,
    message: Mutex<String>, // jamais verrouillé par les callbacks de données
}

fn load_f32(a: &AtomicU32) -> f32 {
    f32::from_bits(a.load(Relaxed))
}

// ───────────────────────── DSP (début) ─────────────────────────

const TAPS: usize = 64;
const PHASES: usize = 512;
const KAISER_BETA: f64 = 8.0;

/// Fonction de Bessel I0 (fenêtre de Kaiser).
fn bessel_i0(x: f64) -> f64 {
    let (mut sum, mut term) = (1.0, 1.0);
    for k in 1..32 {
        term *= (x / (2.0 * k as f64)).powi(2);
        sum += term;
    }
    sum
}

/// Table polyphase sinc fenêtré, calculée une seule fois hors du callback.
fn sinc_table(cutoff: f64) -> Vec<[f32; TAPS]> {
    let half = (TAPS / 2) as f64;
    (0..=PHASES)
        .map(|p| {
            let frac = p as f64 / PHASES as f64;
            let mut row = [0.0f64; TAPS];
            for (k, c) in row.iter_mut().enumerate() {
                let x = k as f64 - (half - 1.0) - frac;
                let a = PI * cutoff * x;
                let sinc = if a.abs() < 1e-9 { 1.0 } else { a.sin() / a };
                let r = (1.0 - (x / half).powi(2)).max(0.0).sqrt();
                *c = cutoff * sinc * bessel_i0(KAISER_BETA * r) / bessel_i0(KAISER_BETA);
            }
            let sum: f64 = row.iter().sum();
            std::array::from_fn(|k| (row[k] / sum) as f32)
        })
        .collect()
}

/// Limiteur doux, utilisé seulement au-delà de 100 % de volume.
fn soft_clip(x: f32) -> f32 {
    const KNEE: f32 = 0.9;
    let a = x.abs();
    if a <= KNEE {
        x
    } else {
        (KNEE + (1.0 - KNEE) * ((a - KNEE) / (1.0 - KNEE)).tanh()).copysign(x)
    }
}

/// Vit dans le callback de sortie : rééchantillonnage + compensation de dérive.
struct Renderer {
    cons: HeapCons<Frame>,
    shared: Arc<Shared>,
    table: Vec<[f32; TAPS]>,
    history: [Frame; TAPS * 2], // doublé pour lire une tranche contiguë
    write: usize,
    frac: f64,
    base_step: f64, // fréquence entrée / fréquence sortie
    in_rate: f64,
    out_rate: f64,
    primed: bool,
    resync: bool,
    fill_avg: f64,
    integral: f64,
    envelope: f32, // fondu anti-clic
    gain: f32,
}

impl Renderer {
    fn new(cons: HeapCons<Frame>, shared: Arc<Shared>, in_rate: u32, out_rate: u32) -> Self {
        let ratio = out_rate as f64 / in_rate as f64;
        Self {
            cons,
            shared,
            table: sinc_table(0.93 * ratio.min(1.0)), // anti-repliement inclus
            history: [[0.0; 2]; TAPS * 2],
            write: 0,
            frac: 0.0,
            base_step: in_rate as f64 / out_rate as f64,
            in_rate: in_rate as f64,
            out_rate: out_rate as f64,
            primed: false,
            resync: false,
            fill_avg: 0.0,
            integral: 0.0,
            envelope: 0.0,
            gain: 0.0,
        }
    }

    #[inline]
    fn push_history(&mut self, f: Frame) {
        self.history[self.write] = f;
        self.history[self.write + TAPS] = f;
        self.write = (self.write + 1) % TAPS;
    }

    #[inline]
    fn interpolate(&self) -> Frame {
        let pos = self.frac * PHASES as f64;
        let i = (pos as usize).min(PHASES - 1);
        let t = (pos - i as f64) as f32;
        let (a, b) = (&self.table[i], &self.table[i + 1]);
        let h = &self.history[self.write..self.write + TAPS];
        let mut out = [0.0f32; 2];
        for k in 0..TAPS {
            let c = a[k] + (b[k] - a[k]) * t;
            out[0] += h[k][0] * c;
            out[1] += h[k][1] * c;
        }
        out
    }

    /// Remplit `frames` trames. Aucune allocation, aucun verrou.
    fn render(&mut self, frames: usize, mut emit: impl FnMut(usize, Frame)) {
        let fill = self.cons.occupied_len();
        let need = frames as f64 * self.base_step;
        let chunk = self.shared.in_chunk.load(Relaxed) as f64;
        let wanted = self.shared.latency_ms.load(Relaxed) as f64 * self.in_rate / 1000.0;
        // La cible couvre toujours un bloc d'entrée + un bloc de sortie + 3 ms de marge.
        let target = wanted.max(chunk + need + 0.003 * self.in_rate);
        let dt = frames as f64 / self.out_rate;

        if !self.primed && chunk > 0.0 && fill as f64 >= target {
            self.cons.skip(fill - target as usize);
            self.primed = true;
            self.resync = false;
            self.fill_avg = target;
        } else if self.primed {
            // Excès important (blocage, ou passage à une latence plus basse).
            if fill as f64 > target + (0.060 * self.in_rate).max(chunk * 2.0) {
                self.resync = true;
            }
            // Asservissement PI : corrige l'écart entre les deux horloges (±0,5 % max).
            self.fill_avg += (fill as f64 - self.fill_avg) * (dt / 0.5).min(1.0);
            let error = (self.fill_avg - target) / self.in_rate; // secondes
            self.integral = (self.integral + 0.0025 * error * dt).clamp(-0.002, 0.002);
        }
        let error = (self.fill_avg - target) / self.in_rate;
        let correction = if self.primed {
            (0.1 * error + self.integral).clamp(-0.005, 0.005)
        } else {
            0.0
        };
        let step = self.base_step * (1.0 + correction);
        self.shared
            .buffer_us
            .store((self.fill_avg / self.in_rate * 1e6) as u32, Relaxed);

        let target_gain = load_f32(&self.shared.gain);
        let gain_coeff = (1.0 - (-1.0 / (0.005 * self.out_rate)).exp()) as f32;
        let fade_step = (1.0 / (0.003 * self.out_rate)) as f32;
        let fade_reserve = (0.004 * self.in_rate) as usize + 2;

        for n in 0..frames {
            if !self.primed {
                emit(n, [0.0; 2]);
                continue;
            }
            // Fondu sortant AVANT que le buffer soit vide : pas de clic.
            let starving = self.cons.occupied_len() < fade_reserve;
            self.envelope = if starving || self.resync {
                (self.envelope - fade_step).max(0.0)
            } else {
                (self.envelope + fade_step).min(1.0)
            };

            if self.envelope == 0.0 && self.resync {
                let excess = self.cons.occupied_len().saturating_sub(target as usize);
                self.cons.skip(excess);
                self.fill_avg = target;
                self.resync = false;
                self.shared.dropouts.fetch_add(1, Relaxed);
            }

            self.frac += step;
            while self.frac >= 1.0 {
                match self.cons.try_pop() {
                    Some(f) => {
                        self.push_history(f);
                        self.frac -= 1.0;
                    }
                    None => {
                        self.primed = false;
                        self.envelope = 0.0;
                        self.frac = 0.0;
                        self.shared.dropouts.fetch_add(1, Relaxed);
                        break;
                    }
                }
            }

            self.gain += (target_gain - self.gain) * gain_coeff;
            let amp = self.gain * self.envelope;
            let boost = self.gain > 1.0;
            let s = self.interpolate().map(|v| {
                let v = v * amp;
                let v = if boost { soft_clip(v) } else { v };
                v.clamp(-1.0, 0.999_999)
            });
            emit(n, s);
        }
    }
}

// ───────────────────────── DSP (fin) ─────────────────────────

// ───────────────────────── Flux cpal ─────────────────────────

fn report(shared: &Shared, side: &str, e: cpal::Error) {
    if matches!(
        e.kind(),
        cpal::ErrorKind::DeviceNotAvailable | cpal::ErrorKind::StreamInvalidated
    ) {
        shared.fatal.store(true, Relaxed);
    }
    // Priorité temps réel refusée : le flux fonctionne quand même.
    if !matches!(e.kind(), cpal::ErrorKind::RealtimeDenied) {
        if let Ok(mut m) = shared.message.lock() {
            *m = format!("{side} : {e}");
        }
    }
}

fn input_stream<T>(
    device: &cpal::Device,
    config: cpal::StreamConfig,
    mut prod: HeapProd<Frame>,
    shared: Arc<Shared>,
) -> Result<cpal::Stream, cpal::Error>
where
    T: SizedSample + Send + 'static,
    f32: FromSample<T>,
{
    let channels = config.channels as usize;
    let errors = shared.clone();
    device.build_input_stream(
        config,
        move |data: &[T], _| {
            let (mut peak_l, mut peak_r) = (0.0f32, 0.0f32);
            let mut overflow = false;
            shared
                .in_chunk
                .fetch_max((data.len() / channels) as u32, Relaxed);
            for frame in data.chunks_exact(channels) {
                let read = |i: usize| {
                    let v = <f32 as FromSample<T>>::from_sample_(frame[i]);
                    if v.is_finite() { v } else { 0.0 }
                };
                let l = read(0);
                let r = if channels > 1 { read(1) } else { l };
                peak_l = peak_l.max(l.abs());
                peak_r = peak_r.max(r.abs());
                overflow |= prod.try_push([l, r]).is_err();
            }
            if overflow {
                shared.dropouts.fetch_add(1, Relaxed);
            }
            shared.peak_l.fetch_max(peak_l.to_bits(), Relaxed);
            shared.peak_r.fetch_max(peak_r.to_bits(), Relaxed);
        },
        move |e| report(&errors, "Entrée", e),
        None,
    )
}

fn output_stream<T>(
    device: &cpal::Device,
    config: cpal::StreamConfig,
    mut renderer: Renderer,
    shared: Arc<Shared>,
) -> Result<cpal::Stream, cpal::Error>
where
    T: SizedSample + FromSample<f32> + Send + 'static,
{
    let channels = config.channels as usize;
    device.build_output_stream(
        config,
        move |data: &mut [T], _| {
            let frames = data.len() / channels;
            renderer.render(frames, |n, [l, r]| {
                let out = &mut data[n * channels..(n + 1) * channels];
                for (c, sample) in out.iter_mut().enumerate() {
                    let v = match (channels, c) {
                        (1, _) => 0.5 * (l + r),
                        (_, 0) => l,
                        (_, 1) => r,
                        _ => 0.0, // surround : seuls les canaux avant G/D sont utilisés
                    };
                    *sample = T::from_sample_(v);
                }
            });
        },
        move |e| report(&shared, "Sortie", e),
        None,
    )
}

macro_rules! dispatch {
    ($format:expr, $func:ident, $($arg:expr),+) => {
        match $format {
            SampleFormat::F32 => $func::<f32>($($arg),+),
            SampleFormat::F64 => $func::<f64>($($arg),+),
            SampleFormat::I16 => $func::<i16>($($arg),+),
            SampleFormat::I24 => $func::<cpal::I24>($($arg),+),
            SampleFormat::I32 => $func::<i32>($($arg),+),
            SampleFormat::U8 => $func::<u8>($($arg),+),
            SampleFormat::U16 => $func::<u16>($($arg),+),
            other => return Err(format!("Format audio non géré : {other}")),
        }
    };
}

struct Route {
    _input: cpal::Stream,
    _output: cpal::Stream,
    info: String,
}

fn start_route(
    input: &cpal::Device,
    output: &cpal::Device,
    shared: Arc<Shared>,
) -> Result<Route, String> {
    // Formats natifs du mélangeur Windows (mode partagé WASAPI).
    let in_cfg = input
        .default_input_config()
        .map_err(|e| format!("Config d'entrée : {e}"))?;
    let out_cfg = output
        .default_output_config()
        .map_err(|e| format!("Config de sortie : {e}"))?;
    let (in_rate, out_rate) = (in_cfg.sample_rate(), out_cfg.sample_rate());

    shared.fatal.store(false, Relaxed);
    shared.in_chunk.store(0, Relaxed);
    shared.dropouts.store(0, Relaxed);
    shared.buffer_us.store(0, Relaxed);
    if let Ok(mut m) = shared.message.lock() {
        m.clear();
    }

    // 1 seconde de capacité ; la latence réelle dépend de la cible, pas de cette taille.
    let (prod, cons) = HeapRb::<Frame>::new(in_rate as usize).split();
    let renderer = Renderer::new(cons, shared.clone(), in_rate, out_rate);

    let in_stream = dispatch!(
        in_cfg.sample_format(),
        input_stream,
        input,
        in_cfg.config(),
        prod,
        shared.clone()
    )
    .map_err(|e| format!("Ouverture de l'entrée : {e}"))?;

    let out_stream = dispatch!(
        out_cfg.sample_format(),
        output_stream,
        output,
        out_cfg.config(),
        renderer,
        shared.clone()
    )
    .map_err(|e| format!("Ouverture de la sortie : {e}"))?;

    in_stream
        .play()
        .map_err(|e| format!("Démarrage entrée : {e}"))?;
    out_stream
        .play()
        .map_err(|e| format!("Démarrage sortie : {e}"))?;

    let khz = |r: u32| format!("{} kHz", r as f32 / 1000.0);
    Ok(Route {
        info: format!(
            "Entrée {} · {} ch   →   Sortie {} · {} ch",
            khz(in_rate),
            in_cfg.channels(),
            khz(out_rate),
            out_cfg.channels()
        ),
        _input: in_stream,
        _output: out_stream,
    })
}

// ───────────────────────── Interface ─────────────────────────

#[derive(Clone, Copy, PartialEq)]
enum Latency {
    Low,
    Balanced,
    Stable,
}

impl Latency {
    const ALL: [Self; 3] = [Self::Low, Self::Balanced, Self::Stable];

    fn ms(self) -> u32 {
        match self {
            Self::Low => 15,
            Self::Balanced => 40,
            Self::Stable => 100,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Low => "Faible latence",
            Self::Balanced => "Équilibré",
            Self::Stable => "Stable",
        }
    }
}

type DeviceList = Vec<(String, cpal::Device)>;

fn list_devices(devices: impl Iterator<Item = cpal::Device>) -> DeviceList {
    let mut list: DeviceList = Vec::new();
    for device in devices {
        let base = device
            .description()
            .map(|d| d.name().to_string())
            .unwrap_or_else(|_| "Périphérique inconnu".into());
        let mut name = base.clone();
        let mut n = 2;
        while list.iter().any(|(existing, _)| *existing == name) {
            name = format!("{base} ({n})");
            n += 1;
        }
        list.push((name, device));
    }
    list
}

/// Garde la sélection par nom, sinon prend le périphérique par défaut.
fn pick(list: &DeviceList, previous: Option<String>, default: Option<cpal::Device>) -> usize {
    let default = default.and_then(|d| d.description().ok().map(|x| x.name().to_string()));
    previous
        .into_iter()
        .chain(default)
        .find_map(|wanted| list.iter().position(|(n, _)| *n == wanted))
        .unwrap_or(0)
}

struct App {
    host: cpal::Host,
    inputs: DeviceList,
    outputs: DeviceList,
    input: usize,
    output: usize,
    volume: f32,
    muted: bool,
    latency: Latency,
    shared: Arc<Shared>,
    route: Option<Route>,
    levels: [f32; 2],
    error: String,
}

impl App {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        cc.egui_ctx.set_theme(egui::Theme::Dark);
        let mut visuals = egui::Visuals::dark();
        visuals.panel_fill = egui::Color32::from_rgb(18, 19, 23);
        visuals.selection.bg_fill = egui::Color32::from_rgb(56, 104, 220);
        cc.egui_ctx.set_visuals_of(egui::Theme::Dark, visuals);

        let mut app = Self {
            host: cpal::default_host(), // WASAPI sous Windows
            inputs: Vec::new(),
            outputs: Vec::new(),
            input: 0,
            output: 0,
            volume: 100.0,
            muted: false,
            latency: Latency::Balanced,
            shared: Arc::new(Shared::default()),
            route: None,
            levels: [0.0; 2],
            error: String::new(),
        };
        app.apply_settings();
        app.refresh();
        app
    }

    fn refresh(&mut self) {
        let old_in = self.inputs.get(self.input).map(|d| d.0.clone());
        let old_out = self.outputs.get(self.output).map(|d| d.0.clone());
        match self.host.input_devices() {
            Ok(d) => self.inputs = list_devices(d),
            Err(e) => self.error = format!("Liste des entrées : {e}"),
        }
        match self.host.output_devices() {
            Ok(d) => self.outputs = list_devices(d),
            Err(e) => self.error = format!("Liste des sorties : {e}"),
        }
        self.input = pick(&self.inputs, old_in, self.host.default_input_device());
        self.output = pick(&self.outputs, old_out, self.host.default_output_device());
    }

    fn apply_settings(&self) {
        let gain = if self.muted { 0.0 } else { self.volume / 100.0 };
        self.shared.gain.store(gain.to_bits(), Relaxed);
        self.shared.latency_ms.store(self.latency.ms(), Relaxed);
    }

    fn start(&mut self) {
        self.error.clear();
        let (Some(i), Some(o)) = (self.inputs.get(self.input), self.outputs.get(self.output))
        else {
            self.error = "Aucun périphérique d'entrée ou de sortie disponible.".into();
            return;
        };
        match start_route(&i.1, &o.1, self.shared.clone()) {
            Ok(route) => self.route = Some(route),
            Err(e) => self.error = e,
        }
    }
}

fn device_combo(ui: &mut egui::Ui, id: &str, list: &DeviceList, selected: &mut usize) {
    let current = list.get(*selected).map_or("Aucun périphérique", |d| &d.0);
    egui::ComboBox::from_id_salt(id)
        .width(ui.available_width())
        .truncate()
        .selected_text(current)
        .show_ui(ui, |ui| {
            for (i, (name, _)) in list.iter().enumerate() {
                ui.selectable_value(selected, i, name.as_str());
            }
        });
}

/// VU-mètre en crête, échelle de -60 à 0 dBFS.
fn meter(ui: &mut egui::Ui, label: &str, level: f32) {
    ui.horizontal(|ui| {
        ui.label(label);
        let size = egui::vec2(ui.available_width() - 62.0, 12.0);
        let (rect, _) = ui.allocate_exact_size(size, egui::Sense::hover());
        let db = 20.0 * level.max(1e-6).log10();
        let ratio = ((db + 60.0) / 60.0).clamp(0.0, 1.0);
        let color = if db > -3.0 {
            egui::Color32::from_rgb(235, 75, 75)
        } else if db > -12.0 {
            egui::Color32::from_rgb(235, 190, 60)
        } else {
            egui::Color32::from_rgb(65, 200, 120)
        };
        ui.painter()
            .rect_filled(rect, 3.0, egui::Color32::from_rgb(38, 40, 48));
        let mut bar = rect;
        bar.set_width(rect.width() * ratio);
        ui.painter().rect_filled(bar, 3.0, color);
        ui.monospace(if db <= -60.0 {
            "  -∞ ".to_string()
        } else {
            format!("{db:>4.0}dB")
        });
    });
}

impl eframe::App for App {
    fn logic(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {
        // Périphérique débranché ou flux invalidé : arrêt propre.
        if self.shared.fatal.swap(false, Relaxed) {
            self.route = None;
            if let Ok(m) = self.shared.message.lock() {
                self.error = format!("Routage arrêté. {m}");
            }
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::CentralPanel::default().show(ui, |ui| {
            ui.spacing_mut().item_spacing.y = 8.0;
            ui.heading("🎧 Routeur Audio");
            ui.separator();

            let running = self.route.is_some();
            ui.add_enabled_ui(!running, |ui| {
                ui.label("Entrée (carte d'acquisition)");
                device_combo(ui, "input", &self.inputs, &mut self.input);
                ui.label("Sortie (casque / haut-parleurs)");
                device_combo(ui, "output", &self.outputs, &mut self.output);
                if ui.button("🔄 Actualiser les périphériques").clicked() {
                    self.error.clear();
                    self.refresh();
                }
            });

            ui.separator();
            ui.horizontal(|ui| {
                ui.label("Volume");
                ui.spacing_mut().slider_width = 220.0;
                ui.add(
                    egui::Slider::new(&mut self.volume, 0.0..=150.0)
                        .suffix(" %")
                        .fixed_decimals(0),
                );
                ui.toggle_value(&mut self.muted, "🔇 Mute");
            });
            ui.horizontal(|ui| {
                ui.label("Latence");
                for mode in Latency::ALL {
                    ui.selectable_value(&mut self.latency, mode, mode.label());
                }
            });
            self.apply_settings();

            ui.separator();
            let peaks = [&self.shared.peak_l, &self.shared.peak_r]
                .map(|a| f32::from_bits(a.swap(0, Relaxed)));
            let decay = (-ui.input(|i| i.stable_dt).min(0.1) / 0.25).exp();
            for (level, peak) in self.levels.iter_mut().zip(peaks) {
                *level = if running {
                    peak.max(*level * decay)
                } else {
                    0.0
                };
            }
            ui.label("Niveau d'entrée");
            meter(ui, "G", self.levels[0]);
            meter(ui, "D", self.levels[1]);

            ui.add_space(4.0);
            let (text, color) = if running {
                ("■  Arrêter", egui::Color32::from_rgb(170, 55, 60))
            } else {
                ("▶  Démarrer", egui::Color32::from_rgb(40, 130, 80))
            };
            let button = egui::Button::new(
                egui::RichText::new(text)
                    .size(18.0)
                    .strong()
                    .color(egui::Color32::WHITE),
            )
            .fill(color);
            if ui.add_sized([ui.available_width(), 40.0], button).clicked() {
                if running {
                    self.route = None; // libère les deux flux
                } else {
                    self.start();
                }
            }

            if let Some(route) = &self.route {
                ui.label(route.info.as_str());
                ui.small(format!(
                    "Tampon interne : {:.0} ms   ·   Décrochages : {}",
                    self.shared.buffer_us.load(Relaxed) as f32 / 1000.0,
                    self.shared.dropouts.load(Relaxed)
                ));
                if let Ok(m) = self.shared.message.try_lock() {
                    if !m.is_empty() {
                        ui.colored_label(egui::Color32::YELLOW, m.as_str());
                    }
                }
            } else {
                ui.weak("Routage arrêté");
            }
            if !self.error.is_empty() {
                ui.colored_label(egui::Color32::from_rgb(255, 110, 110), self.error.as_str());
            }
        });

        // ~30 images/s en marche ; à l'arrêt, egui ne redessine que sur interaction.
        if self.route.is_some() {
            ui.ctx().request_repaint_after(Duration::from_millis(33));
        }
    }
}

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([460.0, 520.0])
            .with_min_inner_size([420.0, 500.0]),
        ..Default::default()
    };
    eframe::run_native(
        "Routeur Audio",
        options,
        Box::new(|cc| Ok(Box::new(App::new(cc)))),
    )
}
