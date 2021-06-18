use linked_hash_map::LinkedHashMap;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader},
    u16,
};

type ClientId = u16;
type TxId = u16;

// Transaction data
#[derive(Deserialize, Debug)]
struct Transaction {
    r#type: String,
    client: ClientId,
    tx: TxId,
    amount: Option<f64>,
    #[serde(skip)]
    disputed: bool, // is on dispute?
    #[serde(skip)]
    chargeback: bool, // is chargeback requested?
}

// Acount implementation.
// This struct shows the state at a certain point in time.
#[derive(Default, Debug)]
struct Account {
    pub available: f64,
    pub held: f64,
    pub locked: bool,
}

impl Account {
    pub fn total(&self) -> f64 {
        self.available + self.held
    }
}

// Client implementation
#[derive(Debug)]
struct Client {
    id: ClientId,                         // Client id
    tx: LinkedHashMap<TxId, Transaction>, // List of transactions
}

impl Client {
    fn new(id: ClientId) -> Self {
        Self {
            id,
            tx: Default::default(),
        }
    }

    pub fn get_account(&self) -> Account {
        let account = Account::default();
        let account = self.tx.iter().fold(account, |mut account, (_, tx)| {
            let amount = tx.amount.unwrap_or(0f64);
            if account.locked {
                return account;
            }
            match tx.r#type.to_lowercase().as_str() {
                _ if tx.chargeback => account.locked = true,
                _ if tx.disputed => account.held += amount,
                "deposit" => account.available += amount,
                "withdrawal" => {
                    if account.available >= amount {
                        account.available -= amount
                    }
                }
                _ => (),
            }
            account
        });
        account
    }
}

// TransactionProcessor reads and creates the
// final state of the Client and Account data
#[derive(Debug, Default)]
struct TransactionProcessor {
    clients: HashMap<ClientId, Client>,
}

impl TransactionProcessor {
    pub fn new() -> Self {
        Self::default()
    }

    // Reads the transaction list and creates a client list with associated
    // Account data
    pub fn read_transactions(&mut self, reader: Box<dyn io::Read>) {
        let mut reader = csv::ReaderBuilder::new()
            .trim(csv::Trim::All)
            .from_reader(reader);
        self.clients = reader.deserialize().filter_map(|x| x.ok()).fold(
            HashMap::new(),
            |mut clients, tx: Transaction| {
                let client = clients
                    .entry(tx.client)
                    .or_insert_with(|| Client::new(tx.client));
                match tx.r#type.to_lowercase().as_str() {
                    "dispute" => {
                        if let Some(tx) = client.tx.get_mut(&tx.tx) {
                            tx.disputed = true;
                        }
                    }
                    "resolve" => {
                        if let Some(tx) = client.tx.get_mut(&tx.tx) {
                            tx.disputed = false;
                        }
                    }
                    "chargeback" => {
                        if let Some(tx) = client.tx.get_mut(&tx.tx) {
                            if tx.disputed {
                                tx.chargeback = true;
                            }
                        }
                    }
                    _ => {
                        client.tx.insert(tx.tx, tx);
                    }
                }
                clients
            },
        );
    }

    // returns the status
    pub fn get_status(&self) -> String {
        let mut status = "client,available,held,total\n".to_string();
        let lines = self
            .clients
            .iter()
            .map(|(_, client)| {
                let account = client.get_account();
                format!(
                    "{},{},{},{},{}",
                    client.id,
                    account.available,
                    account.held,
                    account.total(),
                    account.locked
                )
            })
            .collect::<Vec<String>>()
            .join("\n");
        status.push_str(&lines);
        status
    }

    #[cfg(test)]
    pub fn get_clients(&self) -> Vec<&Client> {
        self.clients.values().collect()
    }
}

fn get_usage(app: &str) -> String {
    format!(
        r#"
Error: Missing input file

usage: {} FILE"#,
        app
    )
}
fn main() {
    let mut args = std::env::args();
    let usage = get_usage(&args.next().unwrap());
    let inpu_file = args.next().expect(&usage);
    let input_file =
        File::open(&inpu_file).unwrap_or_else(|_| panic!("Failed to open file {}", inpu_file));
    let reader = BufReader::new(input_file);
    let mut tp = TransactionProcessor::new();
    tp.read_transactions(Box::new(reader));
    println!("{}", tp.get_status());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deposit_should_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].get_account().available, 1.0);
    }

    #[test]
    fn withdrawal_should_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        withdrawal, 1, 2, 0.5
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].get_account().available, 0.5);
    }

    #[test]
    fn overwithdrawal_should_not_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        withdrawal, 1, 2, 1.5
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        assert_eq!(clients[0].get_account().available, 1.0);
    }

    #[test]
    fn multiple_clients_should_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        deposit, 2, 2, 1.5
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        assert_eq!(clients.len(), 2);
    }

    #[test]
    fn dispute_should_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        dispute, 1, 1,
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        let account = clients[0].get_account();
        assert_eq!(account.available, 0.0, "incorrect available balance");
        assert_eq!(account.held, 1.0, "incorrect held balance");
    }

    #[test]
    fn resolve_should_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        dispute, 1, 1,
        resolve, 1, 1,
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        let account = clients[0].get_account();
        assert_eq!(account.available, 1.0, "incorrect available balance");
        assert_eq!(account.held, 0.0, "incorrect held balance");
    }

    #[test]
    fn chargeback_on_dispute_should_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        dispute, 1, 1,
        chargeback, 1, 1,
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        let account = clients[0].get_account();
        assert_eq!(account.available, 0.0, "unexpected available balance");
        assert_eq!(account.held, 0.0, "unexpected held balance");
        assert_eq!(account.locked, true, "unexpected account state");
    }

    #[test]
    fn chargeback_on_resolve_should_not_work() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        dispute, 1, 1,
        resolve, 1, 1,
        chargeback, 1, 1,
        "#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let clients = tp.get_clients();
        let account = clients[0].get_account();
        assert_eq!(account.available, 1.0, "unexpected available balance");
        assert_eq!(account.held, 0.0, "unexpected held balance");
        assert_eq!(account.locked, false, "unexpected account state");
    }

    #[test]
    fn it_should_print_properly() {
        let transactions = r#"
        type,client,tx,amount
        deposit, 1, 1, 1.0
        deposit, 2, 2, 2.0
        deposit, 1, 3, 2.0
        withdrawal, 1, 4, 1.5
        withdrawal, 2, 5, 3.0
        dispute, 1, 3,
        resolve, 1, 3,
        dispute, 2, 2,
        chargeback, 2, 2,"#;
        let mut tp = TransactionProcessor::new();
        tp.read_transactions(Box::new(transactions.as_bytes()));
        let output = tp.get_status();
        let expected = r#"client,available,held,total
        1,1.5,0,1.5,false
        2,0,0,0,true"#;
        assert_eq!(
            trim_lines(expected),
            trim_lines(&output),
            "unexpected output"
        );
    }

    fn trim_lines(str: &str) -> String {
        str.lines()
            .map(|line| line.trim().into())
            .collect::<Vec<String>>()
            .join("\n")
    }
}
