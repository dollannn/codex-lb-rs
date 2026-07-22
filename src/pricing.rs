use crate::models::UsageData;

/// Date on which the embedded OpenAI API prices were verified.
pub const PRICING_AS_OF: &str = "2026-07-22";
/// Official source for the embedded OpenAI API prices.
pub const PRICING_SOURCE: &str = "https://developers.openai.com/api/docs/pricing";
/// Scope of the estimate. This is not ChatGPT subscription or Codex credit billing.
pub const PRICING_BASIS: &str = "OpenAI Standard API-equivalent text-token pricing in USD; normalizes recognized requests to Standard service-tier rates and excludes regional processing uplifts, tools, and other separately billed features";
/// Stable identifier suitable for persisting alongside an estimate.
///
/// Changing this value or its rate card requires a migration that rebuilds persisted
/// request estimates and account aggregates.
pub const PRICING_VERSION: &str = "openai-standard-2026-07-22";
/// Prompt caching is unavailable below this total input size.
pub const MIN_CACHEABLE_INPUT_TOKENS: i64 = 1_024;
/// The long-context uplift applies only when total input is strictly greater than this.
pub const LONG_CONTEXT_INPUT_THRESHOLD: i64 = 272_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiCostStatus {
    /// Every required token category was present and the result is exact for this rate card.
    Complete,
    /// Cache-write usage was absent; the result bounds all possible cache-write allocations.
    MissingCacheWrite,
    /// The effective model is absent or is not covered by this rate card.
    UnknownModel,
    /// One or more required usage counters are absent.
    MissingUsage,
    /// Usage counters are inconsistent, negative, or too large for a persisted `i64` estimate.
    InvalidUsage,
}

impl ApiCostStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::MissingCacheWrite => "missing_cache_write",
            Self::UnknownModel => "unknown_model",
            Self::MissingUsage => "missing_usage",
            Self::InvalidUsage => "invalid_usage",
        }
    }

    pub const fn is_priced(self) -> bool {
        matches!(self, Self::Complete | Self::MissingCacheWrite)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiCostEstimate {
    pub status: ApiCostStatus,
    /// Inclusive lower estimate in billionths of one US dollar.
    pub lower_nano_usd: Option<i64>,
    /// Inclusive upper estimate in billionths of one US dollar.
    pub upper_nano_usd: Option<i64>,
}

impl ApiCostEstimate {
    const fn unpriced(status: ApiCostStatus) -> Self {
        Self {
            status,
            lower_nano_usd: None,
            upper_nano_usd: None,
        }
    }

    const fn priced(status: ApiCostStatus, lower_nano_usd: i64, upper_nano_usd: i64) -> Self {
        Self {
            status,
            lower_nano_usd: Some(lower_nano_usd),
            upper_nano_usd: Some(upper_nano_usd),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct TokenRates {
    // At the published prices, USD per million tokens converts exactly to nano-USD per token.
    input: i64,
    cached_input: i64,
    cache_write_input: i64,
    output: i64,
}

const SOL_RATES: TokenRates = TokenRates {
    input: 5_000,
    cached_input: 500,
    cache_write_input: 6_250,
    output: 30_000,
};

const TERRA_RATES: TokenRates = TokenRates {
    input: 2_500,
    cached_input: 250,
    cache_write_input: 3_125,
    output: 15_000,
};

const LUNA_RATES: TokenRates = TokenRates {
    input: 1_000,
    cached_input: 100,
    cache_write_input: 1_250,
    output: 6_000,
};

/// Estimate one request at OpenAI's Standard API text-token rates.
///
/// `input_tokens` is treated as the total input count. Cached-read and cache-write
/// counters are disjoint subsets of that total. `reasoning_tokens` is deliberately
/// ignored because it is already included in `output_tokens`. The effective service
/// tier is retained as telemetry but deliberately ignored here: this is a Standard-rate
/// counterfactual, not a reconstruction of the request's actual bill.
pub fn estimate_standard_api_cost(
    effective_model: Option<&str>,
    usage: &UsageData,
) -> ApiCostEstimate {
    let Some(rates) = effective_model.and_then(token_rates) else {
        return ApiCostEstimate::unpriced(ApiCostStatus::UnknownModel);
    };

    let (Some(input), Some(output), Some(cached_input)) = (
        usage.input_tokens,
        usage.output_tokens,
        usage.cached_input_tokens,
    ) else {
        return ApiCostEstimate::unpriced(ApiCostStatus::MissingUsage);
    };
    if input < 0 || output < 0 || cached_input < 0 || cached_input > input {
        return ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage);
    }

    let cache_write_input_tokens = if input < MIN_CACHEABLE_INPUT_TOKENS {
        if cached_input != 0
            || usage
                .cache_write_input_tokens
                .is_some_and(|cache_write| cache_write != 0)
        {
            return ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage);
        }
        Some(0)
    } else {
        usage.cache_write_input_tokens
    };

    let long_context = input > LONG_CONTEXT_INPUT_THRESHOLD;
    match cache_write_input_tokens {
        Some(cache_write_input) => {
            if cache_write_input < 0
                || cached_input
                    .checked_add(cache_write_input)
                    .is_none_or(|accounted| accounted > input)
            {
                return ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage);
            }
            let uncached_input = input - cached_input - cache_write_input;
            match calculate_cost(
                rates,
                uncached_input,
                cached_input,
                cache_write_input,
                output,
                long_context,
            ) {
                Some(cost) => ApiCostEstimate::priced(ApiCostStatus::Complete, cost, cost),
                None => ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage),
            }
        }
        None => {
            let potentially_written = input - cached_input;
            let lower = calculate_cost(
                rates,
                potentially_written,
                cached_input,
                0,
                output,
                long_context,
            );
            let upper = calculate_cost(
                rates,
                0,
                cached_input,
                potentially_written,
                output,
                long_context,
            );
            match (lower, upper) {
                (Some(lower), Some(upper)) => {
                    ApiCostEstimate::priced(ApiCostStatus::MissingCacheWrite, lower, upper)
                }
                _ => ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage),
            }
        }
    }
}

fn token_rates(model: &str) -> Option<TokenRates> {
    match model {
        "gpt-5.6" | "gpt-5.6-sol" => Some(SOL_RATES),
        "gpt-5.6-terra" => Some(TERRA_RATES),
        "gpt-5.6-luna" => Some(LUNA_RATES),
        _ => None,
    }
}

fn calculate_cost(
    rates: TokenRates,
    uncached_input: i64,
    cached_input: i64,
    cache_write_input: i64,
    output: i64,
    long_context: bool,
) -> Option<i64> {
    let input_multiplier = if long_context { 2 } else { 1 };
    // Represent 1.5x exactly as a rational multiplier so no floating-point rounding is involved.
    let (output_numerator, output_denominator) = if long_context { (3, 2) } else { (1, 1) };

    let uncached_cost = checked_category_cost(uncached_input, rates.input, input_multiplier, 1)?;
    let cached_cost = checked_category_cost(cached_input, rates.cached_input, input_multiplier, 1)?;
    let cache_write_cost = checked_category_cost(
        cache_write_input,
        rates.cache_write_input,
        input_multiplier,
        1,
    )?;
    let output_cost =
        checked_category_cost(output, rates.output, output_numerator, output_denominator)?;

    uncached_cost
        .checked_add(cached_cost)?
        .checked_add(cache_write_cost)?
        .checked_add(output_cost)
}

fn checked_category_cost(
    tokens: i64,
    nano_usd_per_token: i64,
    multiplier_numerator: i64,
    multiplier_denominator: i64,
) -> Option<i64> {
    debug_assert!(tokens >= 0);
    debug_assert!(nano_usd_per_token >= 0);
    debug_assert!(multiplier_denominator > 0);
    let numerator =
        i128::from(tokens) * i128::from(nano_usd_per_token) * i128::from(multiplier_numerator);
    let value = numerator.checked_div(i128::from(multiplier_denominator))?;
    i64::try_from(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn usage(
        input: Option<i64>,
        cached: Option<i64>,
        cache_write: Option<i64>,
        output: Option<i64>,
    ) -> UsageData {
        UsageData {
            input_tokens: input,
            output_tokens: output,
            cached_input_tokens: cached,
            cache_write_input_tokens: cache_write,
            reasoning_tokens: None,
            ..UsageData::default()
        }
    }

    #[test]
    fn standard_rate_cards_are_exact_in_nano_usd() {
        let usage = usage(Some(3_000), Some(1_000), Some(1_000), Some(1_000));

        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 41_750_000, 41_750_000)
        );
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-terra"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 20_875_000, 20_875_000)
        );
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-luna"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 8_350_000, 8_350_000)
        );
    }

    #[test]
    fn unsuffixed_gpt_5_6_is_the_sol_alias() {
        let usage = usage(Some(2_000), Some(200), Some(300), Some(100));
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6"), &usage),
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage)
        );
    }

    #[test]
    fn long_context_uplift_is_strictly_greater_than_272k() {
        let at_threshold = usage(Some(272_000), Some(0), Some(0), Some(1_000));
        let above_threshold = usage(Some(272_001), Some(0), Some(0), Some(1_000));

        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &at_threshold),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 1_390_000_000, 1_390_000_000)
        );
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &above_threshold),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 2_765_010_000, 2_765_010_000)
        );
    }

    #[test]
    fn long_context_uplift_covers_every_input_category_and_output() {
        let usage = usage(Some(300_000), Some(100_000), Some(100_000), Some(10_000));
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 2_800_000_000, 2_800_000_000)
        );
    }

    #[test]
    fn absent_cache_write_count_produces_tight_bounds() {
        let usage = usage(Some(2_000), Some(200), None, Some(100));
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::MissingCacheWrite, 12_100_000, 14_350_000)
        );
    }

    #[test]
    fn sub_minimum_requests_have_no_cache_write_uncertainty() {
        let usage = usage(Some(1_023), Some(0), None, Some(100));
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::Complete, 8_115_000, 8_115_000)
        );
    }

    #[test]
    fn sub_minimum_cache_activity_is_invalid() {
        for usage in [
            usage(Some(1_023), Some(1), None, Some(100)),
            usage(Some(1_023), Some(0), Some(1), Some(100)),
        ] {
            assert_eq!(
                estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
                ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage)
            );
        }
    }

    #[test]
    fn missing_cache_write_range_also_uses_long_context_rates() {
        let usage = usage(Some(273_000), Some(73_000), None, Some(1_000));
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-luna"), &usage),
            ApiCostEstimate::priced(ApiCostStatus::MissingCacheWrite, 423_600_000, 523_600_000)
        );
    }

    #[test]
    fn an_absent_but_impossible_cache_write_still_discloses_partial_telemetry() {
        let usage = usage(Some(1_024), Some(1_024), None, Some(10));
        let estimate = estimate_standard_api_cost(Some("gpt-5.6-terra"), &usage);
        assert_eq!(estimate.status, ApiCostStatus::MissingCacheWrite);
        assert_eq!(estimate.lower_nano_usd, estimate.upper_nano_usd);
    }

    #[test]
    fn unknown_or_missing_models_are_unpriced() {
        let usage = usage(Some(10), Some(0), Some(0), Some(10));
        for model in [
            None,
            Some(""),
            Some("gpt-5.5"),
            Some("GPT-5.6-SOL"),
            Some(" gpt-5.6-sol "),
            Some("gpt-5.6-sol-2026-07-01"),
        ] {
            assert_eq!(
                estimate_standard_api_cost(model, &usage),
                ApiCostEstimate::unpriced(ApiCostStatus::UnknownModel)
            );
        }
    }

    #[test]
    fn required_missing_usage_is_unpriced() {
        for usage in [
            usage(None, Some(0), Some(0), Some(0)),
            usage(Some(0), None, Some(0), Some(0)),
            usage(Some(0), Some(0), Some(0), None),
        ] {
            assert_eq!(
                estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
                ApiCostEstimate::unpriced(ApiCostStatus::MissingUsage)
            );
        }
    }

    #[test]
    fn negative_or_inconsistent_usage_is_invalid() {
        for usage in [
            usage(Some(-1), Some(0), Some(0), Some(0)),
            usage(Some(1), Some(-1), Some(0), Some(0)),
            usage(Some(1), Some(0), Some(-1), Some(0)),
            usage(Some(1), Some(0), Some(0), Some(-1)),
            usage(Some(10), Some(11), Some(0), Some(0)),
            usage(Some(10), Some(6), Some(5), Some(0)),
        ] {
            assert_eq!(
                estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
                ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage)
            );
        }
    }

    #[test]
    fn arithmetic_overflow_is_invalid_instead_of_wrapping() {
        let usage = usage(Some(i64::MAX), Some(0), Some(0), Some(0));
        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &usage),
            ApiCostEstimate::unpriced(ApiCostStatus::InvalidUsage)
        );
    }

    #[test]
    fn reasoning_tokens_are_not_charged_a_second_time_or_validated() {
        let mut first = usage(Some(2_000), Some(200), Some(300), Some(50));
        first.reasoning_tokens = Some(-123);
        let mut second = first.clone();
        second.reasoning_tokens = Some(i64::MAX);

        assert_eq!(
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &first),
            estimate_standard_api_cost(Some("gpt-5.6-sol"), &second)
        );
    }

    #[test]
    fn status_strings_and_priced_classification_are_stable() {
        assert_eq!(ApiCostStatus::Complete.as_str(), "complete");
        assert_eq!(
            ApiCostStatus::MissingCacheWrite.as_str(),
            "missing_cache_write"
        );
        assert_eq!(ApiCostStatus::UnknownModel.as_str(), "unknown_model");
        assert_eq!(ApiCostStatus::MissingUsage.as_str(), "missing_usage");
        assert_eq!(ApiCostStatus::InvalidUsage.as_str(), "invalid_usage");
        assert!(ApiCostStatus::Complete.is_priced());
        assert!(ApiCostStatus::MissingCacheWrite.is_priced());
        assert!(!ApiCostStatus::UnknownModel.is_priced());
    }
}
