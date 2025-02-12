#![allow(unused)]
use core::str;
use std::{
    io::{self, Stdout},
    ops::ControlFlow,
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use layout::Flex;
use log::{info, warn, Level};
use ratatui::{prelude::*, widgets::*};
use shared::{de_u32, de_usize, Blueprint, ManagerStatus, PopEvaluation};
use style::{Styled, Stylize};
use text::ToSpan;

use crate::logger::{self, TUILog};

#[derive(PartialEq, Eq)]
pub enum AppState {
    Active,
    Done,
    Aborted,
    Crashed,
}

pub struct App {
    // exp data
    experiment_id: u64,
    start_time: Instant,
    end_duration: Option<Duration>,
    // exp state
    cur_gen: usize,
    num_gens: usize,
    agents_done: usize,
    population_size: usize,
    avg_fitnesses: Vec<(f64, f64)>,
    best_fitnesses: Vec<(f64, f64)>,
    max_fitness: f64,
    // client state
    state: AppState,
    show_help: bool,
    show_abort: bool,
    // logs
    logs: Arc<Mutex<Vec<TUILog>>>,
}

impl App {
    pub fn new(
        blueprint: &Blueprint,
        experiment_id: u64,
        logs: Arc<Mutex<Vec<TUILog>>>,
    ) -> io::Result<Self> {
        let population_size = blueprint.ga.population_size;

        Ok(Self {
            experiment_id,
            start_time: Instant::now(),
            end_duration: None,
            cur_gen: 1,
            num_gens: blueprint.ga.num_generations,
            agents_done: 0,
            population_size,
            avg_fitnesses: Vec::with_capacity(population_size),
            best_fitnesses: Vec::with_capacity(population_size),
            max_fitness: 0.0,
            state: AppState::Active,
            show_help: false,
            show_abort: false,
            logs,
        })
    }

    pub fn run<T>(&mut self, bthread_sock: &zmq::Socket, bthread: &JoinHandle<T>) -> Result<()> {
        let mut terminal = ratatui::init();

        loop {
            match self.tui_loop(&mut terminal, &bthread_sock) {
                Ok(ControlFlow::Break(_)) => break,
                Ok(ControlFlow::Continue(_)) => (),
                Err(e) => {
                    // TODO: this is not chill
                    // panic!("TUI error: {}", e)
                    break;
                }
            }
        }

        ratatui::restore();
        logger::flush(Arc::clone(&self.logs));
        Ok(())
    }

    fn tui_loop(
        &mut self,
        terminal: &mut ratatui::DefaultTerminal,
        bthread_sock: &zmq::Socket,
    ) -> Result<ControlFlow<()>> {
        if event::poll(Duration::from_millis(100))? {
            let event = event::read()?;
            match event_keycode(&event) {
                Some(KeyCode::Char('q')) => match self.state {
                    AppState::Active => {
                        if self.show_abort {
                            bthread_sock.send("abort", 0)?;
                            self.end_duration = Some(self.start_time.elapsed());
                            self.state = AppState::Aborted;
                        }
                        self.show_abort ^= true;
                    }
                    _ => {
                        return Ok(ControlFlow::Break(()));
                    }
                },
                Some(KeyCode::Esc) => {
                    if self.show_abort {
                        self.show_abort = false;
                    }
                }
                // KeyCode::Char('h') => {
                //     self.show_help ^= true;
                // }
                _ => (),
            }
        }

        if self.state == AppState::Active {
            while let Ok(msgb) = bthread_sock.recv_multipart(zmq::DONTWAIT) {
                let prog_type = msgb[0].as_slice();
                match prog_type {
                    b"gen" => {
                        let status = msgb[2].as_slice();
                        match status {
                            b"running" => {
                                self.cur_gen = de_usize(&msgb[1])?;
                                self.agents_done = 0;
                            }
                            b"done" => {
                                let evaluation: PopEvaluation = bincode::deserialize(&msgb[3])?;
                                self.avg_fitnesses.push((
                                    (self.avg_fitnesses.len() + 1) as f64,
                                    evaluation.avg_fitness,
                                ));
                                self.best_fitnesses.push((
                                    (self.best_fitnesses.len() + 1) as f64,
                                    evaluation.best_fitness,
                                ));

                                self.max_fitness = self.max_fitness.max(evaluation.best_fitness);
                            }
                            _ => continue,
                        }
                    }
                    b"agent" => {
                        let status = msgb[2].as_slice();
                        if status == b"done" {
                            self.agents_done += 1;
                        }
                    }
                    b"manager" => {
                        let mgid = de_u32(&msgb[1])?;
                        let status: ManagerStatus = bincode::deserialize(&msgb[2])?;
                        match status {
                            ManagerStatus::Pulling => {
                                info!("🐋 Manager {:x} is pulling the docker image...", mgid)
                            }
                            ManagerStatus::Crashed => warn!("🐋 Manager {:x} crashed", mgid),
                            ManagerStatus::Active => {
                                info!("🐋 Manager {:x} spawned your containers...", mgid)
                            }
                            ManagerStatus::Idle => {
                                info!("🐋 Manager {:x} is loafing around...", mgid)
                            }
                        }
                    }
                    b"done" => {
                        self.end_duration = Some(self.start_time.elapsed());
                        self.state = AppState::Done;
                    }
                    b"crashed" => {
                        self.end_duration = Some(self.start_time.elapsed());
                        self.state = AppState::Crashed;
                    }
                    _ => continue,
                }
            }
        }

        terminal.draw(|frame| frame.render_widget(&*self, frame.area()))?;

        Ok(ControlFlow::Continue(()))
    }
}

impl WidgetRef for App {
    fn render_ref(&self, area: Rect, buf: &mut Buffer) {
        // Layout
        let [header_area, main_area, progress_area] = Layout::vertical([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(3),
        ])
        .areas(area);

        // Header
        let [status_area, title_area, help_area] = Layout::horizontal([
            Constraint::Length(25),
            Constraint::Fill(1),
            Constraint::Length(25),
        ])
        .areas(header_area);

        let state_text = match self.state {
            AppState::Active => "RUNNING".green(),
            AppState::Done => "FINISHED".light_green(),
            AppState::Aborted => "ABORTED".light_yellow(),
            AppState::Crashed => "CRASHED".light_red(),
        };

        Paragraph::new(Line::from(vec![
            state_text.bold(),
            " | ".to_span(),
            format_duration(
                self.end_duration
                    .unwrap_or_else(|| self.start_time.elapsed()),
            )
            .gray(),
        ]))
        .block(Block::new().padding(Padding::uniform(1)))
        .render(status_area, buf);

        Paragraph::new("Squid".bold().light_yellow())
            .centered()
            .block(Block::new().padding(Padding::uniform(1)))
            .render(title_area, buf);

        Paragraph::new(Line::from(vec![
            "Press ".to_span(),
            "Q".bold(),
            " to quit".to_span(),
        ]))
        .right_aligned()
        .block(Block::new().padding(Padding::uniform(1)))
        .render(help_area, buf);

        // Main
        let [other_area, chart_area] =
            Layout::horizontal([Constraint::Percentage(30), Constraint::Fill(1)]).areas(main_area);
        let [info_area, logs_area] =
            Layout::vertical([Constraint::Length(5), Constraint::Fill(1)]).areas(other_area);

        // Info
        let lines = vec![
            info_line("Experiment ID : ", format!("{:x}", self.experiment_id)),
            info_line(
                "Generation    : ",
                format!("{} / {}", self.cur_gen, self.num_gens),
            ),
            info_line(
                "Agents        : ",
                format!("{} / {}", self.agents_done, self.population_size),
            ),
        ];

        Paragraph::new(lines)
            .block(section_block("Info"))
            .render(info_area, buf);

        // Logs
        let logs = self.logs.lock().unwrap();
        let lines = logs
            .iter()
            .rev()
            .take(20)
            .rev()
            .map(log_line)
            .collect::<Vec<_>>();

        Paragraph::new(lines)
            .block(section_block("Logs"))
            .wrap(Wrap { trim: true })
            .render(logs_area, buf);

        // Chart
        let datasets = vec![
            Dataset::default()
                .name("Avg Fitness")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .blue()
                .data(&self.avg_fitnesses),
            Dataset::default()
                .name("Best Fitness")
                .marker(symbols::Marker::Braille)
                .graph_type(GraphType::Line)
                .light_blue()
                .data(&self.best_fitnesses),
        ];

        let chart = Chart::new(datasets)
            .block(section_block("Performance"))
            .x_axis(
                Axis::default()
                    .title("Generation".yellow())
                    .dark_gray()
                    .bounds([1.0, self.num_gens as f64])
                    .labels(["1".into(), self.num_gens.to_string()]),
            )
            .y_axis(
                Axis::default()
                    .title("Fitness".yellow())
                    .dark_gray()
                    .bounds([0.0, self.max_fitness])
                    .labels(["0.0".into(), format!("{:.2}", self.max_fitness)]),
            )
            .render(chart_area, buf);

        // Progress
        let g = self.cur_gen as f64;
        let ng = self.num_gens as f64;
        let a = self.agents_done as f64;
        let na = self.population_size as f64;
        let ratio = (g - 1.0 + (a / na)) / ng;
        let label = format!("{:.2}%", ratio * 100.0);
        Gauge::default()
            .block(section_block("Progress"))
            .gauge_style(match self.state {
                AppState::Active | AppState::Done => Color::LightYellow,
                AppState::Aborted | AppState::Crashed => Color::DarkGray,
            })
            .ratio(ratio.min(1.0))
            .label(label)
            .render(progress_area, buf);

        // Abort confirmation popup
        if self.show_abort {
            let confirmation_text = "Quitting now will abort the experiment";
            let vertical = Layout::vertical([Constraint::Length(7)]).flex(Flex::Center);
            let horizontal = Layout::horizontal([Constraint::Length(60)]).flex(Flex::Center);
            let [area] = vertical.areas(area);
            let [area] = horizontal.areas(area);

            let lines = vec![
                Line::from(confirmation_text).white().bold(),
                "".into(),
                Line::from("q to confirm / esc to cancel").white(),
            ];

            Clear.render(area, buf);
            Paragraph::new(lines)
                .block(
                    Block::bordered()
                        .border_type(BorderType::Double)
                        .padding(Padding::proportional(1))
                        .red()
                        .title(Line::from("WARNING").light_red().bold().centered()),
                )
                .centered()
                .render(area, buf);
        }
    }
}

/// Takes an event, checks if it is a key press event, and returns the [`KeyCode`]
fn event_keycode(event: &Event) -> Option<KeyCode> {
    let Event::Key(key) = event else {
        return None;
    };
    if key.kind != KeyEventKind::Press {
        return None;
    }
    Some(key.code)
}

fn section_block(title: &str) -> Block {
    Block::bordered()
        .border_type(BorderType::Rounded)
        .title(Line::from(title).centered())
}

fn info_line(key: &'static str, value: String) -> Line<'static> {
    Line::from(vec![Span::from(key).bold().yellow(), Span::from(value)])
}

fn info_line_str<'a>(key: &'static str, value: &'a str) -> Line<'a> {
    Line::from(vec![Span::from(key).bold().yellow(), Span::from(value)])
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;

    format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
}

fn log_line(log: &TUILog) -> Line {
    let color = match log.level {
        Level::Error => Color::LightRed,
        Level::Warn => Color::LightYellow,
        _ => Color::White,
    };

    log.content.clone().set_style(color).into()
}
