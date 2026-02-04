from dags.raw_historical_bybit_to_s3 import grub_kline


if __name__ == "__main__":
    start_date = '2025-01-01T00:00:00'
    grub_kline(start_date)