from copy import deepcopy
import csv
from typing import List
import fire
from pydantic import BaseModel
from datetime import date, datetime
from enum import Enum

from pydantic.fields import T, Field


class TransactionType(Enum):
    Receive = "Receive"
    Convert = "Convert"
    Send = "Send"


class TransactionModel(BaseModel):
    timestamp: datetime = Field(None, alias="Timestamp")
    transaction_type: TransactionType = Field(None, alias="Transaction Type")
    asset: str = Field(None, alias="Asset")
    quantity_transacted: float = Field(None, alias="Quantity Transacted")
    spot_price: float = Field(None, alias="ZAR Spot Price at Transaction")
    notes: str = Field(None, alias="Notes")


def _read_csv(filepath: str):
    with open(filepath) as csvfile:
        skip = False
        while not skip:
            skip = csvfile.readline().startswith("Transactions")
            if skip:
                csvfile.readline()
                csvfile.readline()
        transactions = csv.DictReader(csvfile, delimiter=",")
        return [TransactionModel.parse_obj(tx) for tx in transactions]


def _split_conversion(txns: List[TransactionModel] = []):
    for tx in txns:
        if tx.transaction_type == TransactionType.Convert:
            st = f"Converted {tx.quantity_transacted} {tx.asset} to "
            parsed = tx.notes.removeprefix(st).split(sep=" ")
            tx.transaction_type = TransactionType.Send
            value_transacted = tx.spot_price * tx.quantity_transacted
            yield tx
            new_tx = deepcopy(tx)
            new_tx.quantity_transacted = float(parsed[0])
            new_tx.asset = parsed[1]
            new_tx.transaction_type = TransactionType.Receive
            new_tx.spot_price = value_transacted / new_tx.quantity_transacted
            yield new_tx
        else:
            yield tx


def _filter_transaction_type(
    transaction_type: str = None, txns: List[TransactionModel] = []
):
    return [
        tx
        for tx in txns
        if tx.transaction_type == transaction_type or transaction_type == None
    ]


def _filter_asset(asset: str = None, txns: List[TransactionModel] = []):
    return [tx for tx in txns if tx.asset == asset or asset == None]


def _filter_end_date(date: datetime = None, txns: List[TransactionModel] = []):
    return [tx for tx in txns if tx.timestamp.date() <= date or date == None]


def parse(
    filepath: str,
    type: TransactionType = None,
    asset: str = None,
    end_date: date = None,
):
    """
    filepath: path to coinbase csv
    """
    type = TransactionType(type) if type else None
    txns = _read_csv(filepath)
    txns = _filter_end_date(end_date, txns)
    txns = _filter_transaction_type(type, txns)
    txns = _filter_asset(asset, txns)
    txns = _split_conversion(txns)
    return sorted(txns, key=lambda x: x.timestamp)


class Cli(object):
    """A simple calculator class."""

    def sum(
        self,
        filepath: str,
        type: TransactionType = None,
        asset: str = None,
        end_date: date = None,
    ):
        txns = parse(filepath, type, asset)
        total = 0
        for tx in txns:
            total += tx.spot_price * tx.quantity_transacted
        print(total)

    def cg(self, filepath: str, asset: str = None, end_date: date = None):
        """capital gains"""
        end_date = date.fromisoformat(end_date) if end_date else None
        txns = parse(filepath, asset, end_date=end_date)
        gains = {}
        asset_value = {}
        for tx in txns:
            if gains.get(tx.asset) is None:
                gains[tx.asset] = 0
                asset_value[tx.asset] = 0
            first_purchase = asset_value.get(tx.asset) == 0
            actual_gain = 0
            if not first_purchase:
                asset_gain = tx.spot_price - asset_value[tx.asset]
                actual_gain = asset_gain * tx.quantity_transacted
            # print(f"ASSET VALUE CHANGE {tx.asset}: {asset_value[tx.asset]} -> {tx.spot_price:.2f}")
            asset_value[tx.asset] = tx.spot_price

            gains[tx.asset] += actual_gain
        print(gains)
        total = 0
        for gain in gains.values():
            total += gain
        print(f"Total Capital Gains: {total:.2f}")

    def view(
        self,
        filepath: str,
        type: TransactionType = None,
        asset: str = None,
        end_date: date = None,
    ):
        """balance"""
        txns = parse(filepath, asset)
        total = {}
        balance = {}
        asset_value = {}
        for tx in txns:
            if not total.get(tx.asset):
                total[tx.asset] = 0

            asset_value[tx.asset] = tx.spot_price
            amount = (
                tx.quantity_transacted
                if tx.transaction_type == TransactionType.Receive
                else -1 * amount
            )
            total[tx.asset] += amount
            balance[tx.asset] = total[tx.asset] * asset_value[tx.asset]
        print(balance)


if __name__ == "__main__":
    fire.Fire(Cli)
