use eyre::Result;
use futures_util::StreamExt;
use log::{error, info, warn};
use serde::Deserialize;
use solana_client::nonblocking::rpc_client::RpcClient; // <--- ИЗМЕНЕНО: Асинхронный RPC клиент
use solana_sdk::{
    commitment_config::CommitmentConfig,
    native_token::LAMPORTS_PER_SOL, // Для удобства отображения баланса в SOL
    pubkey::Pubkey,
    signature::Signature,
    signer::{keypair::Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};
use std::{
    collections::HashMap,
    fs::{self, File}, // fs::read используется для CA сертификата
    str::FromStr,
    time::Duration,
};
use tonic::transport::{Certificate as TonicCertificate, ClientTlsConfig};
use url::Url;
use yellowstone_grpc_client::GeyserGrpcClient;
use yellowstone_grpc_proto::prelude::{
    subscribe_update::UpdateOneof, CommitmentLevel, SubscribeRequest,
    SubscribeRequestFilterBlocksMeta, // Используем для подписки на метаданные блоков
};

#[derive(Debug, Deserialize)]
struct Config {
    sender_keypair_path: String,
    recipient_address: String,
    rpc_url: String, // Предполагается, что это Devnet RPC URL
    geyser_url: String, // Предполагается, что это Mainnet Geyser URL
    geyser_x_token: String,
    transfer_amount_lamports: u64,
    geyser_ca_cert_path: String,
}

fn load_config(path: &str) -> Result<Config> {
    let file = File::open(path)?;
    let config: Config = serde_yaml::from_reader(file)?;
    Ok(config)
}

// Функция теперь асинхронная и использует асинхронный RpcClient
async fn send_sol_transaction_async(
    rpc_client: &RpcClient, // RpcClient теперь solana_client::nonblocking::rpc_client::RpcClient
    sender_keypair: &Keypair,
    recipient_pubkey: &Pubkey,
    amount_lamports: u64,
) -> Result<Signature> {
    info!("send_sol_transaction_async: Creating instruction to transfer {} lamports from {} to {}...",
        amount_lamports, sender_keypair.pubkey(), recipient_pubkey);

    let instruction = system_instruction::transfer(
        &sender_keypair.pubkey(),
        recipient_pubkey,
        amount_lamports,
    );

    info!("send_sol_transaction_async: Getting latest blockhash...");
    let recent_blockhash = rpc_client.get_latest_blockhash().await?; // .await добавлен

    info!("send_sol_transaction_async: Got blockhash: {}. Creating and signing transaction...", recent_blockhash);
    let tx = Transaction::new_signed_with_payer(
        &[instruction],
        Some(&sender_keypair.pubkey()),
        &[sender_keypair],
        recent_blockhash,
    );

    info!("send_sol_transaction_async: Sending and confirming transaction (with spinner)...");
    // Используем _with_spinner для визуальной обратной связи при подтверждении
    let signature = rpc_client
        .send_and_confirm_transaction_with_spinner(&tx)
        .await?; // .await добавлен

    info!("send_sol_transaction_async: Transaction successful! Signature: {}", signature);
    Ok(signature)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    // Если вы хотите убедиться, что main запускается, раскомментируйте следующую строку
    // println!("DEBUG: MAIN FUNCTION ENTERED (Full Geyser + Async RPC version)");
    info!("Starting Yellowstone Geyser block subscriber and SOL transferrer...");

    let config = load_config("config.yaml")?;
    info!(
        "Configuration loaded. RPC URL (for transactions): {}, Geyser URL (for blocks): {}, CA Cert Path: {}",
        config.rpc_url, config.geyser_url, config.geyser_ca_cert_path
    );

    let sender_keypair = solana_sdk::signature::read_keypair_file(&config.sender_keypair_path)
        .map_err(|e| {
            eyre::eyre!(
                "Failed to read sender keypair from {}: {}",
                config.sender_keypair_path,
                e
            )
        })?;
    info!(
        "Sender keypair loaded. Public key (for Devnet transactions): {}", // Уточнили, что для Devnet
        sender_keypair.pubkey()
    );

    let recipient_pubkey = Pubkey::from_str(&config.recipient_address).map_err(|e| {
        eyre::eyre!(
            "Invalid recipient address {}: {}",
            config.recipient_address,
            e
        )
    })?;
    info!("Recipient public key (for Devnet transactions): {}", recipient_pubkey);

    // Создаем АСИНХРОННЫЙ RpcClient
    let rpc_client =
        RpcClient::new_with_commitment(config.rpc_url.clone(), CommitmentConfig::confirmed());
    info!("Async RPC client configured for: {}", config.rpc_url);

    // Проверка баланса отправителя в Devnet (асинхронно)
    match rpc_client.get_balance(&sender_keypair.pubkey()).await {
        Ok(balance) => info!("Current sender balance on {}: {} lamports ({} SOL)", config.rpc_url, balance, balance as f64 / LAMPORTS_PER_SOL as f64),
        Err(e) => warn!("Could not fetch sender balance from {}: {:?}", config.rpc_url, e),
    }


    // --- Логика подключения к Geyser ---
    let geyser_grpc_url_str =
        if !config.geyser_url.starts_with("http://") && !config.geyser_url.starts_with("https://")
        {
            format!("https://{}", config.geyser_url)
        } else {
            config.geyser_url.clone()
        };
    info!(
        "Attempting to connect to Geyser GRPC: {}",
        geyser_grpc_url_str
    );

    let parsed_geyser_url = Url::parse(&geyser_grpc_url_str).map_err(|e| {
        eyre::eyre!(
            "Failed to parse Geyser URL '{}': {}",
            geyser_grpc_url_str,
            e
        )
    })?;
    let domain = parsed_geyser_url
        .host_str()
        .ok_or_else(|| eyre::eyre!("Geyser URL '{}' has no host", geyser_grpc_url_str))?
        .to_string();

    info!(
        "Attempting to load CA certificate from: {}",
        config.geyser_ca_cert_path
    );
    let ca_cert_pem_bytes = fs::read(&config.geyser_ca_cert_path).map_err(|e| {
        eyre::eyre!(
            "Failed to read CA certificate from '{}': {}",
            config.geyser_ca_cert_path,
            e
        )
    })?;
    let ca_certificate = TonicCertificate::from_pem(ca_cert_pem_bytes);
    info!("CA certificate loaded successfully.");

    let tls_config = ClientTlsConfig::new()
        .ca_certificate(ca_certificate)
        .domain_name(domain);

    let max_message_size_bytes = 32 * 1024 * 1024; // 32 MB
    let mut geyser_client = GeyserGrpcClient::build_from_shared(geyser_grpc_url_str.clone())?
        .x_token(Some(config.geyser_x_token.clone()))?
        .connect_timeout(Duration::from_secs(10)) // Тайм-аут на подключение
        .tls_config(tls_config)?
        .max_decoding_message_size(max_message_size_bytes) // Без ?
        .connect() // Этот вызов асинхронный
        .await // Ожидаем подключения
        .map_err(|e| eyre::eyre!("Failed to connect to Geyser GRPC: {:?}", e))?;

    info!("Successfully connected to Geyser GRPC.");

    // Используем blocks_meta для подписки на метаданные блоков
    let mut blocks_meta_filters = HashMap::new();
    blocks_meta_filters.insert(
        "main_block_meta_filter".to_string(),
        SubscribeRequestFilterBlocksMeta::default(), // Фильтр по умолчанию
    );

    let subscribe_request = SubscribeRequest {
        blocks_meta: blocks_meta_filters,
        blocks: HashMap::new(), // Убедимся, что это поле пустое
        commitment: Some(CommitmentLevel::Confirmed.into()),
        ..Default::default()
    };

    let (_sink, mut stream) = geyser_client
        .subscribe_with_request(Some(subscribe_request))
        .await // Ожидаем установки подписки
        .map_err(|e| eyre::eyre!("Failed to subscribe to Geyser: {:?}", e))?;

    info!("Subscribed to Geyser block meta updates. Waiting for new blocks meta (likely from Mainnet)...");

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                info!("Ctrl-C received, shutting down.");
                break;
            }
            message_item = stream.next() => {
                match message_item {
                    Some(Ok(update_message)) => {
                        match update_message.update_oneof {
                            Some(UpdateOneof::BlockMeta(block_meta_info)) => {
                                info!(
                                    "New block metadata detected (from Geyser)! Slot: {}, Blockhash: {}",
                                    block_meta_info.slot, block_meta_info.blockhash
                                );
                                info!(
                                    "Attempting to send {} lamports from {} to {} on Devnet...",
                                    config.transfer_amount_lamports,
                                    sender_keypair.pubkey(),
                                    recipient_pubkey
                                );

                                // Вызываем асинхронную функцию отправки транзакции
                                match send_sol_transaction_async(
                                    &rpc_client,
                                    &sender_keypair,
                                    &recipient_pubkey,
                                    config.transfer_amount_lamports,
                                )
                                .await // Важно: .await здесь
                                {
                                    Ok(signature) => {
                                        // Лог об успехе уже внутри send_sol_transaction_async
                                        info!("Main loop: Transaction to Devnet successful, signature: {}", signature);
                                        // Опционально: проверка балансов после транзакции
                                        if let Ok(balance) = rpc_client.get_balance(&sender_keypair.pubkey()).await {
                                             info!("Sender balance on Devnet after tx: {} SOL", balance as f64 / LAMPORTS_PER_SOL as f64);
                                        }
                                        if let Ok(balance) = rpc_client.get_balance(&recipient_pubkey).await {
                                             info!("Recipient balance on Devnet after tx: {} SOL", balance as f64 / LAMPORTS_PER_SOL as f64);
                                        }
                                    }
                                    Err(e) => {
                                        error!("Main loop: Failed to send SOL transaction to Devnet: {:?}", e);
                                    }
                                }
                            }
                            Some(UpdateOneof::Ping(_)) => {
                                info!("Received Ping from Geyser.");
                            }
                            Some(UpdateOneof::Pong(_)) => {
                                 info!("Received Pong from Geyser.");
                            }
                            Some(other_update_variant) => {
                                warn!("Received other unhandled update type variant from Geyser: {:?}", other_update_variant);
                            }
                            None => {
                                warn!("Received SubscribeUpdate from Geyser with no specific update_oneof content.");
                            }
                        }
                    }
                    Some(Err(status)) => {
                        error!("Error from Geyser stream: {:?}", status);
                        // Возможно, стоит добавить задержку перед попыткой переподключения или просто выйти
                        break; 
                    }
                    None => {
                        info!("Geyser stream ended.");
                        break; 
                    }
                }
            }
        }
    }
    info!("Program finished.");
    Ok(())
}