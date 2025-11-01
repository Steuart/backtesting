from feeddata import fund_feeddata

data = fund_feeddata.load_data("513300.SH", "2023-01-01", "2025-01-01", "1d")
print(data)