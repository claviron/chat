#[cfg(feature = "webtransport")]
use bytes::Buf;
use bytes::{BufMut, Bytes, BytesMut};
use rmp_serde as rmps;
#[cfg(feature = "webtransport")]
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::types::messages::{ClientMessage, ServerMessage};

pub(crate) const FRAME_PREFIX_LEN: usize = 4;

pub(crate) fn decode_client_message(
    payload: &[u8]
) -> Result<ClientMessage, String> {
    rmps::from_slice::<ClientMessage>(payload).map_err(|err| err.to_string())
}

pub(crate) fn encode_server_message(
    msg: &ServerMessage,
    max_frame: usize,
) -> Result<Bytes, String> {
    let payload = rmps::to_vec_named(msg).map_err(|err| err.to_string())?;
    if payload.len() > max_frame {
        return Err(format!(
            "frame too large: {} > {max_frame}",
            payload.len()
        ));
    }
    let mut frame = BytesMut::with_capacity(FRAME_PREFIX_LEN + payload.len());
    frame.put_u32(payload.len() as u32);
    frame.extend_from_slice(&payload);
    Ok(frame.freeze())
}

#[cfg(feature = "webtransport")]
pub(crate) async fn read_client_message<R>(
    reader: &mut R,
    inbound: &mut BytesMut,
    max_frame: usize,
) -> Result<Option<ClientMessage>, String>
where
    R: AsyncRead + Unpin,
{
    loop {
        if inbound.len() >= FRAME_PREFIX_LEN {
            let len = u32::from_be_bytes(
                inbound[..FRAME_PREFIX_LEN]
                    .try_into()
                    .expect("slice length checked"),
            ) as usize;
            if len > max_frame {
                return Err(format!("frame too large: {len} > {max_frame}"));
            }
            if inbound.len() >= FRAME_PREFIX_LEN + len {
                inbound.advance(FRAME_PREFIX_LEN);
                let payload = inbound.split_to(len);
                return decode_client_message(&payload).map(Some);
            }
        }

        let read =
            reader.read_buf(inbound).await.map_err(|err| err.to_string())?;
        if read == 0 {
            return Ok(None);
        }
    }
}

#[cfg(feature = "webtransport")]
pub(crate) async fn write_server_message<W>(
    writer: &mut W,
    msg: ServerMessage,
    max_frame: usize,
) -> Result<(), String>
where
    W: AsyncWrite + Unpin,
{
    let bytes = encode_server_message(&msg, max_frame)?;
    writer.write_all(&bytes).await.map_err(|err| err.to_string())?;
    writer.flush().await.map_err(|err| err.to_string())
}
