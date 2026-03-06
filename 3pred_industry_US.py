import os
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from sklearn.preprocessing import MinMaxScaler
from datetime import timedelta

# Configuration
TRAIN_FILE = "data/dwa/forecast/US_industry_train.csv"
PRED_FILE = "data/dwa/forecast/US_industry_pred.csv"
SEQUENCE_LENGTH = 60
FORECAST_DAYS = 90
EPOCHS = 50
LEARNING_RATE = 0.001
HIDDEN_SIZE = 64
NUM_LAYERS = 2

class LSTMForecaster(nn.Module):
    def __init__(self, input_size, hidden_size, num_layers, output_size):
        super(LSTMForecaster, self).__init__()
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size).to(x.device)
        out, _ = self.lstm(x, (h0, c0))
        out = self.fc(out[:, -1, :])
        return out

def load_and_preprocess_data(filepath):
    df = pd.read_csv(filepath)
    df['activity_date'] = pd.to_datetime(df['activity_date'])
    df = df.sort_values('activity_date').reset_index(drop=True)

    # Fill missing values
    df_numeric = df.drop(columns=['activity_date'])
    df_numeric = df_numeric.ffill().bfill()

    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(df_numeric)

    return df, scaled_data, scaler, list(df_numeric.columns)

def create_sequences(data, seq_length):
    xs = []
    ys = []
    for i in range(len(data) - seq_length):
        x = data[i:(i + seq_length)]
        y = data[i + seq_length]
        xs.append(x)
        ys.append(y)
    return np.array(xs), np.array(ys)

def train_and_predict(scaled_data, scaler, df, feature_cols):
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

    X, y = create_sequences(scaled_data, SEQUENCE_LENGTH)
    X_tensor = torch.tensor(X, dtype=torch.float32).to(device)
    y_tensor = torch.tensor(y, dtype=torch.float32).to(device)

    model = LSTMForecaster(input_size=scaled_data.shape[1], hidden_size=HIDDEN_SIZE, num_layers=NUM_LAYERS, output_size=scaled_data.shape[1]).to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=LEARNING_RATE)

    print("Training LSTM model...")
    for epoch in range(EPOCHS):
        model.train()
        optimizer.zero_grad()
        outputs = model(X_tensor)
        loss = criterion(outputs, y_tensor)
        loss.backward()
        optimizer.step()

        if (epoch + 1) % max(1, (EPOCHS // 10)) == 0:
            print(f"Epoch {epoch+1}/{EPOCHS}, Loss: {loss.item():.4f}")

    # Forecasting
    model.eval()
    current_seq = scaled_data[-SEQUENCE_LENGTH:]
    current_seq_tensor = torch.tensor(current_seq, dtype=torch.float32).unsqueeze(0).to(device)

    predictions = []
    with torch.no_grad():
        for _ in range(FORECAST_DAYS):
            pred = model(current_seq_tensor)
            predictions.append(pred.cpu().numpy()[0])

            # Update sequence for next prediction
            pred_expanded = pred.unsqueeze(1) # shape: (1, 1, num_features)
            current_seq_tensor = torch.cat((current_seq_tensor[:, 1:, :], pred_expanded), dim=1)

    predictions = np.array(predictions)
    predictions_inverse = scaler.inverse_transform(predictions)

    # Create future dates (skip weekends for simplicity, assuming daily business days)
    last_date = df['activity_date'].iloc[-1]
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=FORECAST_DAYS, freq='B')
    if len(future_dates) < FORECAST_DAYS:
         # fallback if B doesn't yield enough days, though it will
         future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=FORECAST_DAYS)

    df_pred = pd.DataFrame(predictions_inverse, columns=feature_cols)
    df_pred.insert(0, 'activity_date', future_dates[:FORECAST_DAYS])

    return df_pred

def main():
    if not os.path.exists(TRAIN_FILE):
        print(f"Error: Training file not found: {TRAIN_FILE}")
        return

    df_train, scaled_data, scaler, feature_cols = load_and_preprocess_data(TRAIN_FILE)

    print("Generating forecast...")
    df_pred = train_and_predict(scaled_data, scaler, df_train, feature_cols)

    os.makedirs(os.path.dirname(PRED_FILE), exist_ok=True)
    df_pred.to_csv(PRED_FILE, index=False)
    print(f"Forecast successfully generated and saved to {PRED_FILE}")

if __name__ == "__main__":
    main()
