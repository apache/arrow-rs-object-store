//! Example demonstrating the use of different cryptographic providers
//! 
//! This example shows how to use both `ring` and `aws-lc-rs` as cryptographic providers.
//! 
//! Run with:
//! ```bash
//! # Using ring (default)
//! cargo run --example crypto_providers --features "cloud"
//! 
//! # Using aws-lc-rs
//! cargo run --example crypto_providers --features "cloud,crypto-aws-lc-rs" --no-default-features
//! ```

fn main() {
    println!("Cryptographic Provider Example");
    
    #[cfg(feature = "crypto-ring")]
    println!("Using ring cryptographic provider");
    
    #[cfg(feature = "crypto-aws-lc-rs")]
    println!("Using aws-lc-rs cryptographic provider");
    
    #[cfg(all(not(feature = "crypto-ring"), not(feature = "crypto-aws-lc-rs")))]
    println!("No cryptographic provider enabled");
    
    // Test basic functionality
    #[cfg(any(feature = "crypto-ring", feature = "crypto-aws-lc-rs"))]
    {
        println!("Crypto provider is available for cloud operations");
        
        // In a real application, you would create object store instances here
        // For example:
        // let s3 = AmazonS3Builder::new()
        //     .with_region("us-east-1")
        //     .with_bucket_name("my-bucket")
        //     .build()
        //     .unwrap();
    }
}
