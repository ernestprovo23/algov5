import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense, Dropout
from alpha_vantage.timeseries import TimeSeries
from io import BytesIO
import credentials
from s3connector import connect_to_storage_account, azure_connection_string,download_blob, upload_blob

# Azure storage account
blob_service_client = connect_to_storage_account(azure_connection_string)

# Alpha Vantage API key
alpha_vantage_api_key = credentials.ALPHA_VANTAGE_API

# Function to download CSV from Azure and convert to dataframe
def download_blob_to_dataframe(blob_service_client, container_name, blob_name):
    print(f"Downloading blob {blob_name} from container {container_name}...")
    container_client = blob_service_client.get_container_client(container_name)
    blob_data = container_client.download_blob(blob_name).readall()
    df = pd.read_csv(BytesIO(blob_data))
    print("Download complete.")
    return df


# Function to upload dataframe as CSV to Azure
def upload_dataframe_to_blob(blob_service_client, container_name, blob_name, df):
    print(f"Uploading dataframe to blob {blob_name} in container {container_name}...")
    container_client = blob_service_client.get_container_client(container_name)
    csv_data = df.to_csv(index=False)
    container_client.upload_blob(name=blob_name, data=csv_data, overwrite=True)
    print("Upload complete.")


# Load symbols from Azure
df_symbols = download_blob_to_dataframe(blob_service_client, "historic", "selected_pairs.csv")

# Alpha Vantage time series object
ts = TimeSeries(key=alpha_vantage_api_key, output_format="pandas")


# Function to preprocess data for LSTM
def preprocess_data(data, lookback):
    print("Preprocessing data...")
    data = np.array(data)
    data = data.reshape(-1, 1)
    scaler = MinMaxScaler(feature_range=(0, 1))
    data = scaler.fit_transform(data)

    x = []
    y = []
    for i in range(lookback, len(data)):
        x.append(data[i - lookback:i, 0])
        y.append(data[i, 0])

    x, y = np.array(x), np.array(y)
    x = np.reshape(x, (x.shape[0], x.shape[1], 1))

    print("Preprocessing complete.")
    return x[:-60], y[:-60], x[-60:], y[-60:], scaler


# Function to build and compile LSTM model
def build_model(input_shape):
    print("Building model...")
    model = Sequential()
    model.add(LSTM(units=50, return_sequences=True, input_shape=input_shape))
    model.add(Dropout(0.2))
    model.add(LSTM(units=50, return_sequences=False))
    model.add(Dropout(0.2))
    model.add(Dense(units=25))
    model.add(Dense(units=1))

    model.compile(optimizer='adam', loss='mean_squared_error')
    print("Model built.")
    return model


# Function to plot history and predictions
def plot_history_and_predictions(history, y_test, y_pred):
    plt.figure(figsize=(14, 5))
    plt.plot(history.history['loss'])
    plt.title('Model Loss Progress')
    plt.xlabel('Epoch')
    plt.ylabel('Training Loss')
    plt.legend(['Training Loss'])
    plt.show()

    plt.figure(figsize=(14, 5))
    plt.plot(y_test, color='blue', label='Real')
    plt.plot(y_pred, color='red', label='Predicted')
    plt.title('Real vs Predicted Price')
    plt.xlabel('Time')
    plt.ylabel('Price')
    plt.legend()
    plt.show()


# Iterate over the symbols
for symbol in df_symbols['Symbol']:
    print(f"\nProcessing symbol {symbol}...")

    # Get daily historical data from AlphaVantage
    print("Downloading historical data from AlphaVantage...")
    data, meta_data = ts.get_daily(symbol, outputsize='full')
    data = data['4. close']  # We're only interested in the closing prices
    data = data[::-1]  # Reverse the data to be in ascending order
    print("Download complete.")

    # Preprocess data
    X_train, y_train, X_test, y_test, scaler = preprocess_data(data, lookback=60)

    # Build LSTM model
    model = build_model(input_shape=(X_train.shape[1], 1))

    # Train LSTM model
    print("Training model...")
    history = model.fit(X_train, y_train, epochs=50, batch_size=32)
    print("Training complete.")

    # Predict future prices
    print("Predicting future prices...")
    y_pred = model.predict(X_test)
    y_pred = scaler.inverse_transform(y_pred)  # Undo scaling
    print("Prediction complete.")

    # Save predictions to a DataFrame
    print("Saving predictions...")
    df_pred = pd.DataFrame({"Real": y_test.flatten(), "Predicted": y_pred.flatten()})
    print("Saving complete.")

    # Upload predictions to Azure
    upload_dataframe_to_blob(blob_service_client, "historic", f"{symbol}_predictions.csv", df_pred)

    # Plot history and predictions
    print("Plotting history and predictions...")
    plot_history_and_predictions(history, y_test, y_pred)
    print("Plotting complete.")

print("\nAll symbols processed.")
