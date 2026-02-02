#[macro_export]
macro_rules! ctx {
    ($msg:expr) => {
        format!("{} [{}:{}]", $msg, file!(), line!())
    };
    () => {
        format("[{}:{}]", file!(), line!())
    };
}
