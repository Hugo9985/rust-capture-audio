# Rust Capture Audio

An ultra-lightweight, native Windows desktop application written in Rust that routes audio from a capture card (or any input device) directly to your headphones or speakers in real time.

Built as a zero-bloat alternative to running OBS Studio solely for audio monitoring.

## Features

- **Low-Latency Audio Engine:** Powered by `cpal` (Windows WASAPI backend) and a lock-free `ringbuf` pipeline for glitch-free real-time playback.
- **Minimalist Native GUI:** Built with `eframe` / `egui` featuring a dark theme and near-zero CPU/GPU footprint.
- **Device Selection:** Quick dropdowns to pick your input source (capture card) and output device (headset/speakers).
- **Audio Controls:** Real-time volume slider (`0%` – `150%`), quick mute toggle, and live VU meter.
- **Configurable Buffer:** Switch between low-latency, balanced, and stable buffer profiles depending on your audio driver.
- **Single Standalone Executable:** Compiles into a portable `.exe` with no background console window.

## Prerequisites

- **Windows 10 / 11**
- **Rust toolchain** (`rustc` and `cargo`) installed via [rustup.rs](https://rustup.rs/) with MSVC C++ Build Tools.

## Build & Run

1. Clone the repository:
   ```bash
   git clone [https://github.com/Hugo9985/rust-capture-audio.git](https://github.com/Hugo9985/rust-capture-audio.git)
   cd rust-capture-audio
