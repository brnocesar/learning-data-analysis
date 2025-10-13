import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score
import statsmodels.api as sm
import joblib

SEED = 14376 # definir isso em um lugar so e importar, para evitar repeticao
np.random.seed(SEED)


df = pd.read_csv('datasets/ml_fundamentos_2_regressao_linear_preco_casas.csv')
df.rename(columns={'quantidade_banheiros': 'num_banheiros', 'qualidade_da_cozinha_Excelente': 'cozinha_excelente', 'preco_de_venda': 'preco'}, inplace=True)
# print(df.shape, '\n', df.head()) # teste

# define colunas usadas como variaveis independentes para modelo
variaveis_independentes = ['area_primeiro_andar', 'existe_segundo_andar', 'num_banheiros', 'cozinha_excelente']

# separa variaveis variaveis independentes (adicionando a constante) e variavel dependente
X = sm.add_constant(df[variaveis_independentes])
Y = df['preco']

# separa dados de treino e teste
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.3)

# treina modelo com dados de treino
modelo = sm.OLS(Y_train, X_train).fit()

# testa modelo com dados de teste
predicoes = modelo.predict(X_test)
# print(f"R²: treino={modelo.rsquared:.3f}\tteste={r2_score(Y_test, predicoes):.3f}") # teste

joblib.dump(modelo, "artifacts/models/preco_casa_sm_OLS.pkl")


# # estima preco de uma nova casa
# print(
#     joblib.load("artifacts/models/preco_casa_sm_OLS.pkl").predict(
#         pd.DataFrame([{'const': 1, 'area_primeiro_andar': 120, 'existe_segundo_andar': 1, 'num_banheiros': 2, 'cozinha_excelente': 0}])
#         )
#     )
