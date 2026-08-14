use aws_ssm_bridge::SessionBuilder;
use futures_util::StreamExt;

let session = SessionBuilder::new("i-0123456789abcdef0").start().await?;
session.wait_ready().await?;

let mut output = session.output();
session.send(&b"uname -a\r"[..]).await?;

while let Some(chunk) = output.next().await {
    print!("{}", String::from_utf8_lossy(&chunk));
}
session.terminate().await?;
