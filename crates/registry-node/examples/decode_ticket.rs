use std::str::FromStr;
fn main() {
    let raw = std::env::args()
        .nth(1)
        .expect("usage: decode_ticket <ticket>");
    match iroh_docs::DocTicket::from_str(&raw) {
        Ok(t) => println!("{t:#?}"),
        Err(e) => println!("DECODE ERROR: {e}"),
    }
}
