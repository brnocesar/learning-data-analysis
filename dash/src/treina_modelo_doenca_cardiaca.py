import numpy as np
from ucimlrepo import fetch_ucirepo
from sklearn.model_selection import train_test_split
import xgboost as xgb
from sklearn.metrics import accuracy_score
import joblib

SEED = 14376 # definir isso em um lugar so e importar, para evitar repeticao
np.random.seed(SEED)


heart_disease = fetch_ucirepo(id=45)
df            = heart_disease.data.features
df['doenca']  = (heart_disease.data.targets > 0) * 1
# print(df.shape) # teste

X = df.drop(columns=["doenca"])
Y = df["doenca"]

X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, stratify=Y)

modelo = xgb.XGBClassifier(objective='binary:logistic')
modelo.fit(X_train, Y_train)
predicoes = modelo.predict(X_test)
acuracia  = accuracy_score(Y_test, predicoes)

# print(f"A acurácia do modelo é de {acuracia*100:.1f}%") # teste

joblib.dump(modelo, "artifacts/models/doenca_cardiaca_xgboost.pkl")

# mediana das colunas numericas: usadas como valores padrao no formulario, para evitar erros de NaN quando o usuario não informar algum valor
medianas = X.median()
joblib.dump(medianas, "artifacts/support/doenca_cardiaca_medianas.pkl")
