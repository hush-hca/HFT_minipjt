use iced::mouse;
use iced::widget::canvas::{self, Canvas};
use iced::{Color, Element, Length, Point, Rectangle, Renderer, Theme};

use super::reducer::Message;

pub fn price_chart<'a>(mid: &'a [(i64, i128)], micro: &'a [(i64, i128)]) -> Element<'a, Message> {
    Canvas::new(PriceChart { mid, micro })
        .width(Length::Fill)
        .height(180)
        .into()
}

struct PriceChart<'a> {
    mid: &'a [(i64, i128)],
    micro: &'a [(i64, i128)],
}

impl<Message> canvas::Program<Message> for PriceChart<'_> {
    type State = ();

    fn draw(
        &self,
        _state: &Self::State,
        renderer: &Renderer,
        _theme: &Theme,
        bounds: Rectangle,
        _cursor: mouse::Cursor,
    ) -> Vec<canvas::Geometry> {
        let mut frame = canvas::Frame::new(renderer, bounds.size());
        frame.fill_rectangle(Point::ORIGIN, bounds.size(), Color::from_rgb8(20, 24, 31));
        let domain = chart_domain(self.mid, self.micro);
        if let Some(domain) = domain {
            draw_series(&mut frame, self.mid, domain, Color::from_rgb8(84, 160, 255));
            draw_series(
                &mut frame,
                self.micro,
                domain,
                Color::from_rgb8(58, 211, 159),
            );
        }
        vec![frame.into_geometry()]
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct ChartDomain {
    min_ts: i64,
    max_ts: i64,
    min_value: i128,
    max_value: i128,
}

fn chart_domain(mid: &[(i64, i128)], micro: &[(i64, i128)]) -> Option<ChartDomain> {
    let mut points = mid.iter().chain(micro);
    let &(first_ts, first_value) = points.next()?;
    let mut domain = ChartDomain {
        min_ts: first_ts,
        max_ts: first_ts,
        min_value: first_value,
        max_value: first_value,
    };
    for &(timestamp, value) in points {
        domain.min_ts = domain.min_ts.min(timestamp);
        domain.max_ts = domain.max_ts.max(timestamp);
        domain.min_value = domain.min_value.min(value);
        domain.max_value = domain.max_value.max(value);
    }
    Some(domain)
}

fn draw_series(
    frame: &mut canvas::Frame,
    points: &[(i64, i128)],
    domain: ChartDomain,
    color: Color,
) {
    if points.is_empty() {
        return;
    }
    let padding = 10.0_f32;
    let width = (frame.width() - 2.0 * padding).max(1.0);
    let height = (frame.height() - 2.0 * padding).max(1.0);
    let ts_span = i128::from(domain.max_ts.saturating_sub(domain.min_ts)).max(1);
    let value_span = domain.max_value.saturating_sub(domain.min_value).max(1);
    let path = canvas::Path::new(|builder| {
        for (index, &(timestamp, value)) in points.iter().enumerate() {
            let x_ratio =
                i128::from(timestamp.saturating_sub(domain.min_ts)) as f64 / ts_span as f64;
            let y_ratio = value.saturating_sub(domain.min_value) as f64 / value_span as f64;
            let point = Point::new(
                padding + width * x_ratio as f32,
                padding + height * (1.0 - y_ratio as f32),
            );
            if index == 0 {
                builder.move_to(point);
            } else {
                builder.line_to(point);
            }
        }
    });
    frame.stroke(
        &path,
        canvas::Stroke::default().with_color(color).with_width(2.0),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_domain_covers_both_series_without_fabricating_points() {
        let mid = [(10, 100), (20, 120)];
        let micro = [(15, 90), (30, 130)];
        assert_eq!(
            chart_domain(&mid, &micro),
            Some(ChartDomain {
                min_ts: 10,
                max_ts: 30,
                min_value: 90,
                max_value: 130,
            })
        );
        assert_eq!(chart_domain(&[], &[]), None);
    }
}
