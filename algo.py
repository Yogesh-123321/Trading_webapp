from dhanhq import dhanhq
import pandas as pd
import yfinance as yf
import datetime
import time
from dotenv import load_dotenv
import os

load_dotenv()

# ---- credentials ----
client_id = os.getenv("client_id")
access_token = os.getenv("access_token")
dhan = dhanhq(client_id, access_token)

def get_instrument_token(stock_name):
    df = pd.read_csv('api-scrip-master.csv')
    data_dict = {}
    for index, row in df.iterrows():
        trading_symbol = row['SEM_TRADING_SYMBOL']
        exm_ecxh_id = row['SEM_EXM_EXCH_ID']
        if trading_symbol not in data_dict:
            data_dict[trading_symbol] = {}
        data_dict[trading_symbol][exm_ecxh_id] = row.to_dict()
    return data_dict[stock_name]['NSE']['SEM_SMST_SECURITY_ID']

def place_order(stock_id, qty, target, stop_loss):
    order = dhan.place_order(
        security_id=stock_id,
        exchange_segment=dhan.NSE,
        transaction_type=dhan.BUY,
        quantity=qty,
        order_type=dhan.MARKET,
        product_type=dhan.INTRA,
        price=0,
        bo_profit_value=target,
        bo_stop_loss_Value=stop_loss)
    return order

def get_day_positions():
    positions = dhan.get_positions()
    print("DEBUG: Raw positions data from Dhan:", positions)  # Debug print
    if not positions or 'data' not in positions or not positions['data']:
        return pd.DataFrame()  # Empty DataFrame if no positions
    return pd.DataFrame(positions['data'])

def close_all_buy_positions():
    df = get_day_positions()
    
    if df.empty:
        print("No positions left to close.")
        return
    
    if 'positionType' not in df.columns or 'netQty' not in df.columns:
        print("Positions data does not have required columns.")
        return
    
    buy_positions = df[(df['positionType'] != 'CLOSED') & (df['netQty'] > 0)]
    
    if buy_positions.empty:
        print("No open BUY positions to close.")
        return

    for index, row in buy_positions.iterrows():
        security_id = row['securityId']
        trading_symbol = row['tradingSymbol']
        quantity = int(row['netQty'])
        product_type = row['productType']
        try:
            response = dhan.place_order(
                security_id=security_id,
                exchange_segment=dhan.NSE,
                transaction_type=dhan.SELL,
                quantity=quantity,
                order_type=dhan.MARKET,
                product_type=product_type,
                price=0)
            print(f"Closed BUY position: {trading_symbol}, Qty: {quantity}")
        except Exception as e:
            print(f"Failed to close position for {trading_symbol}: {e}")

def round_to_tick(price, tick_size=0.05):
    return round(round(price / tick_size) * tick_size, 2)

def get_chart(stock_name):
    stock = yf.Ticker(stock_name + ".NS")
    df = stock.history(interval="5m", period="2d")
    df.reset_index(inplace=True)
    df = df[['Datetime', 'Open', 'High', 'Low', 'Close']]
    df.rename(columns={'Datetime': 'timestamp'}, inplace=True)
    df['timestamp'] = df['timestamp'].dt.tz_localize(None)

    for col in ['Open', 'High', 'Low', 'Close']:
        df[col] = df[col].apply(round_to_tick)

    df['SMA_44'] = df['Close'].rolling(window=44).mean().round(2)
    return df

def sma_rising(stock_id):
    chart = get_chart(stock_id)
    if chart.loc[50, 'SMA_44'] < chart.loc[74, 'SMA_44'] < chart.loc[98, 'SMA_44'] < chart.loc[122, 'SMA_44'] < chart.loc[146, 'SMA_44']:
        return True
    else:
        return False

watchlist = ['NHPC','MOTHERSON','PNB','CANBK','IRFC','UNIONBANK','IOC','TATASTEEL','GAIL','BHEL','ONGC','BANKBARODA','WIPRO','POWERGRID','ECLERX','BPCL','NTPC','COALINDIA','TATAPOWER','BEL','PFC','ITC','VEDL','VBL','DABUR','JSWENERGY','ADANIPOWER','ATGL','AMBUJACEM','ICICIPRULI','TATAMOTORS','HINDALCO','IRCTC','HDFCLIFE','DLF','SBIN','INDUSINDBK','BAJFINANCE','LICI','ADANIGREEN','ZYDUSLIFE','JINDALSTEL','TATACONSUM','JSWSTEEL','AXISBANK','DRREDDY','GODREJCP','LODHA','UBL','ADANIPORTS','NAUKRI','RELIANCE','INFY','ICICIBANK','HCLTECH','TECHM','CHOLAFIN','CIPLA','HAVELLS','SUNPHARMA','SBILIFE','ICICIGI','BAJAJFINSV','BHARTIARTL','KOTAKBANK','HDFCBANK','NESTLEIND','ADANIENT','ASIANPAINT','HINDUNILVR','GRASIM','TVSMOTOR','TCS','PIDILITIND','SIEMENS','M&M','TITAN','TORNTPHARM','LT','DMART','HAL','HEROMOTOCO','LTIM','ABB','TRENT','BRITANNIA','EICHERMOT','INDIGO','DIVISLAB','APOLLOHOSP']

traded_watchlist = []

pd.set_option('display.max_rows', None)

while True:
     # ---- time preferences ----
     current_time = datetime.datetime.now().time()
     if current_time < datetime.time(9, 30):
          print("wait for market to start", current_time)
          time.sleep(2)
          continue
     if current_time > datetime.time(15,00):
          traded_watchlist = []
          close_all_buy_positions()
          print("Market is over, Bye Bye see you tomorrow", current_time)
          time.sleep(3600)
          break

     time.sleep(10)

     # ---- loop for each stock ----
     for stock_name in watchlist:
        # ---- data fetch ----
        chart = get_chart(stock_name)
        stock_id = get_instrument_token(stock_name)
        is_rising = sma_rising(stock_name)

        # ---- bullish candles ----
        engulf = (
            chart.iloc[-3]['Open'] > chart.iloc[-3]['Close'] and
            chart.iloc[-2]['Open'] < chart.iloc[-3]['Close'] and
            chart.iloc[-2]['Close'] > chart.iloc[-3]['Open'])
        red_hammer = (
            chart.iloc[-3]['Close'] < chart.iloc[-3]['Open'] and
            (chart.iloc[-3]['Close'] - chart.iloc[-3]['Low']) >= 3 * abs(chart.iloc[-3]['Open'] - chart.iloc[-3]['Close']))
        green_hammer = (
            chart.iloc[-3]['Close'] > chart.iloc[-3]['Open'] and
            (chart.iloc[-3]['Open'] - chart.iloc[-3]['Low']) >= 3 * abs(chart.iloc[-3]['Close'] - chart.iloc[-3]['Open']))
        white_soldiers = (
            chart.iloc[-3]['Close'] > chart.iloc[-3]['Open'] and
            chart.iloc[-4]['Close'] > chart.iloc[-4]['Open'] and
            chart.iloc[-3]['Close'] > chart.iloc[-4]['Close'] and
            chart.iloc[-4]['Close'] > chart.iloc[-5]['Close'])


        # ---- candle formations ----
        bullish = engulf or red_hammer or green_hammer or white_soldiers
        crossover = (chart.iloc[-2]['Low'] < chart.iloc[-2]['SMA_44']) and (chart.iloc[-2]['High'] > chart.iloc[-2]['SMA_44']) and (chart.iloc[-2]['Open'] < chart.iloc[-2]['Close'])
        confirmation = chart.iloc[-1]['High'] > chart.iloc[-2]['High']

        # ---- trade value calculation ----
        balance_response = dhan.get_fund_limits()
        available_balance = balance_response['data']['availabelBalance']
        leveraged_margin = available_balance * 5
        buy_price = chart.iloc[-1]['High']
        target_get = buy_price + 2.5 * (chart.iloc[-2]['High'] - chart.iloc[-2]['Low'])
        target = round(target_get, 2)
        stop_loss_get = chart.iloc[-4]['Low']
        stop_loss = round(stop_loss_get, 2)
        qty = 1 # int(leveraged_margin // buy_price)

        # ---- trade conditions ----
        if crossover and confirmation and bullish and  stock_name not in traded_watchlist and is_rising and buy_price < leveraged_margin:
            # print(chart)
            place_order(stock_id, qty, target, stop_loss)
            print("Bought", qty, "quantity of", stock_name, "with Avg price:", buy_price, ",Target:", target, ",Stop loss:", stop_loss)
            traded_watchlist.append(stock_name)
            print("Traded stocks:", traded_watchlist)

