use super::model::{ControlAvailability, OperatorCommand, UiSnapshot};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Screen {
    Opportunities,
    Market,
    Strategy,
    Health,
    Risk,
}

#[derive(Debug, Clone)]
pub enum Message {
    Navigate(Screen),
    FilterChanged(String),
    SelectSymbol(String),
    Snapshot(Box<UiSnapshot>),
    PollSnapshot,
    ArmPressed,
    CancelAllPressed,
    ClosePositionsPressed,
    KillPressed,
}

pub struct FundingGuiState {
    pub screen: Screen,
    pub filter: String,
    pub selected_symbol: Option<String>,
    pub snapshot: UiSnapshot,
    pub last_notice: Option<String>,
}

impl FundingGuiState {
    pub fn new(snapshot: UiSnapshot) -> Self {
        let selected_symbol = snapshot.opportunities.first().map(|row| row.symbol.clone());
        Self {
            screen: Screen::Opportunities,
            filter: String::new(),
            selected_symbol,
            snapshot,
            last_notice: None,
        }
    }

    pub fn update(&mut self, message: Message) -> Option<OperatorCommand> {
        match message {
            Message::Navigate(screen) => self.screen = screen,
            Message::FilterChanged(value) => self.filter = value,
            Message::SelectSymbol(symbol) => {
                self.selected_symbol = Some(symbol);
                self.screen = Screen::Market;
            }
            Message::Snapshot(snapshot) => self.snapshot = *snapshot,
            Message::PollSnapshot => {}
            Message::ArmPressed
            | Message::CancelAllPressed
            | Message::ClosePositionsPressed
            | Message::KillPressed => {
                if let ControlAvailability::Disabled { code } = &self.snapshot.risk.availability {
                    self.last_notice = Some(code.clone());
                } else {
                    self.last_notice = Some("CONFIRMATION_REQUIRED".into());
                }
            }
        }
        None
    }

    pub fn visible_opportunities(&self) -> impl Iterator<Item = &super::model::OpportunityRow> {
        let needle = self.filter.to_ascii_uppercase();
        self.snapshot
            .opportunities
            .iter()
            .filter(move |row| row.symbol.to_ascii_uppercase().contains(&needle))
    }
}
