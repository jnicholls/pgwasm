//! WASIp2 guest: outbound HTTPS GET + JSON body (integration fixture for `pg_wasm`).

wit_bindgen::generate!({
    path: "wit",
    world: "http-search",
    generate_all,
});

use crate::wasi::http::outgoing_handler;
use crate::wasi::http::types::{
    ErrorCode, Fields, IncomingBody, IncomingResponse, Method, OutgoingBody, OutgoingRequest,
    Scheme,
};
use crate::wasi::io::poll::Pollable;

struct Component;

impl Guest for Component {
    fn search_titles() -> String {
        match run_search() {
            Ok(s) => s,
            Err(e) => format!("error:{e:?}"),
        }
    }
}

fn run_search() -> Result<String, ErrorCode> {
    let headers = Fields::new();
    let req = OutgoingRequest::new(headers);
    req.set_scheme(Some(&Scheme::Https))
        .map_err(|()| ErrorCode::InternalError(None))?;
    req.set_authority(Some("hn.algolia.com"))
        .map_err(|()| ErrorCode::InternalError(None))?;
    req.set_path_with_query(Some("/api/v1/search?query=postgresql"))
        .map_err(|()| ErrorCode::InternalError(None))?;
    req.set_method(&Method::Get)
        .map_err(|()| ErrorCode::InternalError(None))?;

    let outgoing_body = req.body().map_err(|()| ErrorCode::InternalError(None))?;
    OutgoingBody::finish(outgoing_body, None)?;

    let fut = outgoing_handler::handle(req, None)?;
    block_on(&fut.subscribe());
    let resp = match fut.get() {
        Some(Ok(Ok(r))) => r,
        Some(Ok(Err(e))) => return Err(e),
        Some(Err(_)) => return Err(ErrorCode::InternalError(None)),
        None => return Err(ErrorCode::InternalError(None)),
    };

    let body = read_response_body(resp)?;
    Ok(String::from_utf8_lossy(&body).into_owned())
}

fn read_response_body(resp: IncomingResponse) -> Result<Vec<u8>, ErrorCode> {
    let body = resp
        .consume()
        .map_err(|()| ErrorCode::InternalError(None))?;
    let stream = body.stream().map_err(|()| ErrorCode::InternalError(None))?;
    let mut out = Vec::new();
    loop {
        match stream.blocking_read(64 * 1024) {
            Ok(chunk) => {
                if chunk.is_empty() {
                    break;
                }
                out.extend_from_slice(&chunk);
            }
            Err(crate::wasi::io::streams::StreamError::Closed) => break,
            Err(crate::wasi::io::streams::StreamError::LastOperationFailed(_)) => {
                return Err(ErrorCode::InternalError(None));
            }
        }
    }
    drop(stream);
    let _trailers = IncomingBody::finish(body);
    Ok(out)
}

fn block_on(p: &Pollable) {
    while !p.ready() {
        p.block();
    }
}

export!(Component);
