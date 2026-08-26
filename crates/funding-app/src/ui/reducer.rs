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
    SelectMarket { symbol: String, venue: String },
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
    pub selected_venue: Option<String>,
    pub snapshot: UiSnapshot,
    pub last_notice: Option<String>,
}

impl FundingGuiState {
    pub fn new(snapshot: UiSnapshot) -> Self {
        let selected_symbol = snapshot.opportunities.first().map(|row| row.symbol.clone());
        let selected_venue = snapshot.markets.first().map(|row| row.venue.clone());
        Self {
            screen: Screen::Opportunities,
            filter: String::new(),
            selected_symbol,
            selected_venue,
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
                self.selected_venue = Some("Binance USD-M".into());
                self.screen = Screen::Market;
            }
            Message::SelectMarket { symbol, venue } => {
                self.selected_symbol = Some(symbol);
                self.selected_venue = Some(venue);
                self.screen = Screen::Market;
            }
            Message::Snapshot(snapshot) => {
                self.snapshot = *snapshot;
                if self.selected_symbol.is_none() && !self.snapshot.markets.is_empty() {
                    self.selected_symbol = Some(self.snapshot.market.symbol.clone());
                    self.selected_venue = Some(self.snapshot.market.venue.clone());
                }
            }
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
