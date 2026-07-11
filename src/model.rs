//! ONNX policy inference — one session per category.
//!
//! The SB3 PPO policy is exported (scripts/export_policy_onnx.py, validation
//! phase) as a graph mapping observation [1, 20] f32 -> deterministic action
//! [1, 2] f32. This mirrors `model.predict(obs, deterministic=True)`: the
//! action mean, which SB3 clips to the action space [-1, 1] — we clip here.
//!
//! Categories whose model file is missing or has the wrong input width are
//! DISABLED (never traded), mirroring the Python 20-dim checkpoint guard
//! that protects against feeding a 16/19-dim-era model garbage.

use anyhow::{bail, Context, Result};
use ort::session::Session;
use ort::value::Tensor;

pub const OBS_DIM: usize = 20;
pub const ACT_DIM: usize = 2;

pub struct Policy {
    session: Session,
}

impl Policy {
    /// Load and validate a policy. Fails (category disabled by caller) when
    /// the file is missing or the graph's input width is not OBS_DIM.
    pub fn load(path: &str) -> Result<Self> {
        let session = Session::builder()
            .context("ort session builder")?
            .commit_from_file(path)
            .with_context(|| format!("loading ONNX policy {path}"))?;

        // Input-dim guard: last dimension must be the 20-dim observation.
        let input = session
            .inputs()
            .first()
            .with_context(|| format!("{path}: ONNX graph has no inputs"))?;
        let dims = input
            .dtype()
            .tensor_shape()
            .with_context(|| format!("{path}: input is not a tensor"))?;
        let last = dims.last().copied().unwrap_or(-1);
        if last != OBS_DIM as i64 {
            bail!("{path}: expects obs width {last}, live builder is {OBS_DIM} — category DISABLED until retrained");
        }
        Ok(Self { session })
    }

    /// Deterministic action for one observation, clipped to [-1, 1].
    pub fn predict(&mut self, obs: &[f32; OBS_DIM]) -> Result<[f64; ACT_DIM]> {
        let tensor = Tensor::from_array(([1usize, OBS_DIM], obs.to_vec()))?;
        let outputs = self.session.run(ort::inputs![tensor])?;
        let (_, data) = outputs[0]
            .try_extract_tensor::<f32>()
            .context("policy output is not an f32 tensor")?;
        if data.len() < ACT_DIM {
            bail!("policy output has {} values, expected {ACT_DIM}", data.len());
        }
        Ok([
            f64::from(data[0]).clamp(-1.0, 1.0),
            f64::from(data[1]).clamp(-1.0, 1.0),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_model_file_errors() {
        assert!(Policy::load("definitely/not/a/model.onnx").is_err());
    }
    // Round-trip inference is covered by rust/tests/parity.rs once
    // scripts/export_policy_onnx.py produces real policy files (T12).
}
