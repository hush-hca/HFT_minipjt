use thiserror::Error;

use crate::model::DECIMAL_SCALE;

const MAX_DECIMAL_38: i128 = 99_999_999_999_999_999_999_999_999_999_999_999_999;

#[derive(Debug, Clone, Eq, PartialEq, Error)]
pub enum DecimalError {
    #[error("decimal value is empty")]
    Empty,
    #[error("invalid decimal format")]
    InvalidFormat,
    #[error("decimal has {fractional_digits} fractional digits; at most {max} are allowed")]
    ExcessFractionalPrecision {
        fractional_digits: usize,
        max: usize,
    },
    #[error("decimal value exceeds precision 38 at scale 18")]
    PrecisionOverflow,
}

pub fn parse_decimal_18(text: &str) -> Result<i128, DecimalError> {
    if text.is_empty() {
        return Err(DecimalError::Empty);
    }

    let (negative, unsigned) = match text.as_bytes()[0] {
        b'-' => (true, &text[1..]),
        b'+' => (false, &text[1..]),
        _ => (false, text),
    };
    if unsigned.is_empty() {
        return Err(DecimalError::InvalidFormat);
    }

    let mut parts = unsigned.split('.');
    let integer_text = parts.next().ok_or(DecimalError::InvalidFormat)?;
    let fraction_text = parts.next();
    if parts.next().is_some()
        || integer_text.is_empty()
        || fraction_text.is_some_and(str::is_empty)
        || !integer_text.bytes().all(|byte| byte.is_ascii_digit())
        || fraction_text.is_some_and(|fraction| !fraction.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(DecimalError::InvalidFormat);
    }

    let fraction_text = fraction_text.unwrap_or("");
    let scale = DECIMAL_SCALE as usize;
    if fraction_text.len() > scale {
        return Err(DecimalError::ExcessFractionalPrecision {
            fractional_digits: fraction_text.len(),
            max: scale,
        });
    }

    let integer = parse_digits(integer_text)?;
    let fraction = if fraction_text.is_empty() {
        0
    } else {
        parse_digits(fraction_text)?
    };
    let scale_factor = checked_power_of_ten(scale).ok_or(DecimalError::PrecisionOverflow)?;
    let fraction_factor =
        checked_power_of_ten(scale - fraction_text.len()).ok_or(DecimalError::PrecisionOverflow)?;
    let magnitude = integer
        .checked_mul(scale_factor)
        .and_then(|scaled| {
            fraction
                .checked_mul(fraction_factor)
                .and_then(|fraction| scaled.checked_add(fraction))
        })
        .ok_or(DecimalError::PrecisionOverflow)?;

    if magnitude > MAX_DECIMAL_38 {
        return Err(DecimalError::PrecisionOverflow);
    }

    Ok(if negative { -magnitude } else { magnitude })
}

fn parse_digits(text: &str) -> Result<i128, DecimalError> {
    text.bytes().try_fold(0_i128, |value, byte| {
        value
            .checked_mul(10)
            .and_then(|value| value.checked_add(i128::from(byte - b'0')))
            .ok_or(DecimalError::PrecisionOverflow)
    })
}

fn checked_power_of_ten(exponent: usize) -> Option<i128> {
    (0..exponent).try_fold(1_i128, |value, _| value.checked_mul(10))
}
