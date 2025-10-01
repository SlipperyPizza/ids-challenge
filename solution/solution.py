# Import Libraries

import pandas as pd
from datetime import datetime
import os
import json


# Pastikan panda tidak melakukan trunkasi.
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Masukkan file data dari directory /data
def load_data(dir_path: str) -> list:
    events = []
    if not os.path.exists(dir_path):
        print(f"Error: Directory tidak ditemukan pada path '{dir_path}'")
        return events
    
    for filename in os.listdir(dir_path):
        if filename.endswith(".json"):
            file_path = os.path.join(dir_path, filename)
            with open(file_path, "r") as file:
                try:
                    events.append(json.load(file))
                except json.JSONDecodeError:
                    print(f"Error: File '{file_path}' tidak dapat di-decode sebagai JSON")
    return events

def final_table(events: list) -> pd.DataFrame:
    """
    Membangun kembali (merekonstruksi) kondisi akhir sebuah tabel dari daftar event log.
    
    Fungsi ini memproses event 'create' dan 'update' secara kronologis untuk
    menghasilkan representasi final dari setiap record.
    """
    events.sort(key=lambda x: x["ts"])
    records = {}
    for event in events:
        record_id = event["id"]
        if event['op'] == 'c':
            records[record_id] = event['data']
        elif event['op'] == 'u':
            records[record_id].update(event['set'])
    
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(list(records.values()))

# Menemukan final state tabel
def rekonstruksi_tabel_bertahap(events: list):
    """
    Generator yang menghasilkan (yields) state DataFrame setelah setiap event.
    """
    # events.sort(key=lambda x: x["ts"])
    records = {}
    
    # Proses semua event 'create' terlebih dahulu untuk state awal
    update_events = []
    for event in events:
        record_id = event["id"]
        if event['op'] == 'c':
            records[record_id] = event['data']
        elif event['op'] == 'u':
            update_events.append(event)
    
    # Hasilkan state awal setelah semua 'create'
    if records:
        yield pd.DataFrame.from_records(list(records.values()))
    
    # Proses setiap event 'update' satu per satu dan hasilkan state baru
    for event in update_events:
        record_id = event["id"]
        if record_id in records:
            records[record_id].update(event['set'])
            yield pd.DataFrame.from_records(list(records.values()))

def print_tahapan_tabel(nama_tabel: str, events: list):
    """
    Mencetak setiap tahapan perubahan untuk sebuah tabel.
    """
    print(f"\n--- Tahapan Perubahan untuk: {nama_tabel} ---")
    generator = rekonstruksi_tabel_bertahap(events)
    for i, state in enumerate(generator):
        print(f"\n--- {nama_tabel}: State #{i+1} ---")
        print(state)

def get_all_states(events: list) -> list[pd.DataFrame]:
    """
    Kembalikan semua tahapan tabel dari sebuah event list.
    """
    return list(rekonstruksi_tabel_bertahap(events))


def print_all_states(nama_tabel: str, events: list):
    """
    Cetak semua tahapan tabel dengan lebih rapih.
    """
    states = get_all_states(events)
    if not states:
        print(f"\n{nama_tabel}: Tidak ada state.")
        return
    for i, df in enumerate(states, 1):
        print(f"\n--- {nama_tabel}: State #{i} ---")
        print(df)


def final_table(events: list) -> pd.DataFrame:
    """
    Membangun kembali (merekonstruksi) kondisi akhir sebuah tabel dari daftar event log.
    
    Fungsi ini memproses event 'create' dan 'update' secara kronologis untuk
    menghasilkan representasi final dari setiap record.
    """
    events.sort(key=lambda x: x["ts"])
    records = {}
    for event in events:
        record_id = event["id"]
        if event['op'] == 'c':
            records[record_id] = event['data']
        elif event['op'] == 'u':
            records[record_id].update(event['set'])
    
    if not records:
        return pd.DataFrame()
    return pd.DataFrame.from_records(list(records.values()))


def find(card_events: list, savings_events: list) -> pd.DataFrame:
    """
    Menganalisis event log dari kartu dan tabungan untuk menemukan transaksi keuangan.

    Transaksi didefinisikan sebagai setiap perubahan pada 'credit_used' kartu atau
    'balance' akun tabungan. Fungsi ini tidak akan mencatat transaksi bernilai nol.
    """
    transactions = []
    card_states = {}
    savings_states = {}
    all_events = card_events + savings_events
    all_events.sort(key=lambda x: x["ts"])

    for event in all_events:
        record_id = event["id"]
        is_card = 'card_number' in event.get('data', {})
        is_savings = 'balance' in event.get('data', {})

        if event['op'] == 'c':
            if is_card:
                card_states[record_id] = event['data']
            if is_savings:
                savings_states[record_id] = event['data']

        elif event ['op'] == 'u':
            if 'credit_used' in event['set'] and record_id in card_states:
                old_value = card_states[record_id]['credit_used']
                new_value = event['set']['credit_used']
                trans_value = new_value - old_value

                if trans_value != 0:
                    transactions.append({
                        'timestamp': event['ts'],
                        'account_id': card_states[record_id]['card_id'],
                        'transaction_type': 'Card', 'value': new_value - old_value,
                        'note': f"Credit used from {old_value} to {new_value}"
                    })
                    
                card_states[record_id].update(event['set'])

            elif 'balance' in event['set'] and record_id in savings_states:
                old_value = savings_states[record_id]['balance']
                new_value = event['set']['balance']
                trans_value = new_value - old_value

                if trans_value != 0:
                    transactions.append({
                        'timestamp': event['ts'],
                        'account_id': savings_states[record_id]['savings_account_id'],
                        'transaction_type': 'Savings', 'value': new_value - old_value,
                        'note': f"Balance changed from {old_value} to {new_value}"
                    })
                savings_states[record_id].update(event['set'])

    if not transactions:
        return pd.DataFrame()
    df = pd.DataFrame(transactions)
    df['datetime'] = df['timestamp'].apply(lambda x: datetime.fromtimestamp(x / 1000))
    return df[['datetime', 'account_id', 'transaction_type', 'value', 'note']]

# Memastikan Directory Data
dir_path = "data"


# Load
account_ev = load_data(os.path.join(dir_path, "accounts"))
cards_ev = load_data(os.path.join(dir_path, "cards"))
savings_accounts_ev = load_data(os.path.join(dir_path, "savings_accounts"))

# Task 1
print("Task 1: Historical Table Views")
print("-" * 40)

print_all_states("Accounts Table", account_ev)
print_all_states("Cards Table", cards_ev)
print_all_states("Savings Accounts Table", savings_accounts_ev)


# Task 2
df_acc = final_table(account_ev)
df_cards = final_table(cards_ev)
df_savings = final_table(savings_accounts_ev)

print("Task 2: Latest Table Views")
print("-" * 40)

denormalized = pd.merge(df_acc, df_cards, how="left", on="card_id")
denormalized = pd.merge(denormalized, df_savings, how="left", on="savings_account_id")
denormalized = denormalized.rename(columns={"status_x": "status_card", "status_y": "status_savings"})

print(denormalized)
print("\n" + "-" * 40 + "\n")

# Task 3

print("Task 3: Transaction Analysis")
print("-" * 40)

transactions_df = find(cards_ev, savings_accounts_ev)
print("Detected Transactions: ")
print(transactions_df)
