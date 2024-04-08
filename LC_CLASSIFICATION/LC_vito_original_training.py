
from pathlib import Path
import json
import pandas as pd

from helper import aggregate_csv

from sklearn.ensemble import RandomForestClassifier
from skl2onnx import to_onnx
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import numpy as np

base_output_path = Path("output")
timestr = "20240406-08h47"
UID = "UID"
target_column = "EUGW_LC"
tracking_file = base_output_path / f"tracking_{timestr}.csv"

json_filepath = Path("band_names.json")
with open(str(json_filepath), 'r') as f:
    final_band_names = json.load(f)

final_csv_path = base_output_path.joinpath(f"aggregated_{timestr}.csv")
if not final_csv_path.exists():
    aggregate_csv(final_csv_path, base_output_path, timestr,UID,target_column , final_band_names)
else:
    df = pd.read_csv(final_csv_path)



X = df[final_band_names]
X = X.astype(np.float32)  # convert to float32 to allow ONNX conversion later on
y = df[target_column].astype(int)

# Step 1: Find indices of rows with NaN in df1
nan_indices = X[X.isnull().any(axis=1)].index
# Step 2: Drop these rows from both DataFrames
X_cleaned = X.drop(nan_indices)
y_corresponding = y.drop(nan_indices)
X_train, X_test, y_train, y_test = train_test_split(X_cleaned, y_corresponding, test_size=0.3, random_state=42)
#get unique values and counts of each value
unique, counts = np.unique(y_train, return_counts=True)
print(f"unique {unique}, count {counts}")

rf = RandomForestClassifier(n_estimators=100, max_features=y.unique().size, random_state=40)
rf = rf.fit(X_train, y_train)

y_pred = rf.predict(X_test)
print("Accuracy on test set: " + str(accuracy_score(y_test, y_pred))[0:5])

model_output_path = base_output_path / "models"
model_output_path.mkdir(exist_ok=True)

onnx = to_onnx(model=rf, name="random_forest", X=X_train.values)

with open(base_output_path / "models" / f"random_forest_{timestr}.onnx", "wb") as f:
    f.write(onnx.SerializeToString())
