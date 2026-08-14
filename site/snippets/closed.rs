tokio::select! {
    () = session.closed() => {
        eprintln!("gone: {}", session.close_reason().unwrap());
    }
    result = do_work(&session) => result?,
}
