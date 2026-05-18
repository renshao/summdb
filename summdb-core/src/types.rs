use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub type RepoId = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest(pub [u8; 32]);

impl Digest {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn from_array(arr: [u8; 32]) -> Self {
        Digest(arr)
    }

    pub fn from_slice(slice: &[u8]) -> Result<Self, String> {
        if slice.len() != 32 {
            return Err(format!("expected 32 bytes, got {}", slice.len()));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(slice);
        Ok(Digest(arr))
    }
}

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sha256:")?;
        for b in &self.0 {
            write!(f, "{:02x}", b)?;
        }
        Ok(())
    }
}

impl FromStr for Digest {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hex = s
            .strip_prefix("sha256:")
            .ok_or_else(|| format!("digest must start with 'sha256:': {s}"))?;
        if hex.len() != 64 {
            return Err(format!("sha256 digest must be 64 hex chars: {s}"));
        }
        let mut arr = [0u8; 32];
        for i in 0..32 {
            arr[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|e| format!("invalid hex in digest {s}: {e}"))?;
        }
        Ok(Digest(arr))
    }
}

impl Serialize for Digest {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        if ser.is_human_readable() {
            ser.collect_str(self)
        } else {
            self.0.serialize(ser)
        }
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        if de.is_human_readable() {
            let s = String::deserialize(de)?;
            s.parse().map_err(serde::de::Error::custom)
        } else {
            let arr = <[u8; 32]>::deserialize(de)?;
            Ok(Digest(arr))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Platform {
    pub os: String,
    pub arch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestRecord {
    pub repo: RepoId,
    pub digest: Digest,
    pub media_type: String,
    pub total_layer_size: u64,
    pub platform: Option<Platform>,
    pub layers: Vec<Digest>,
    #[serde(default)]
    pub children: Vec<ChildRef>,
    #[serde(default)]
    pub parent: Option<ManifestRef>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestRef {
    pub repo: RepoId,
    pub digest: Digest,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildRef {
    pub digest: Digest,
    pub platform: Option<Platform>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LayerRecord {
    pub size: u64,
    pub manifests: Vec<ManifestRef>,
}
