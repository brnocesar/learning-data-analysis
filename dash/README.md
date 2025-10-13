# dash-portifolio

## Rodando a aplicação em ambiente local

Após realizar o clone do repositório Git, entre em seu diretório raiz e execute os comandos abaixo:

- Criação do ambiente virtual Python
```terminal
python3 -m venv .venv
```
  - Ativando ambiente virtual no Windows
  ```terminal
  .\.venv\Scripts\activate
  ```
  - Unix
  ```terminal
  source .venv/bin/activate
  ```

- Intalação das bibliotecas necessárias
```terminal
pip install –upgrade pip && pip install -r requirements.txt
```

- Rodar aplicação no servidor local
```terminal
python main.py
```

## Organização do projeto

O projeto está organizado em pastas para facilitar a manutenção e o entendimento do código. A seguir, uma breve descrição das principais pastas e arquivos:

- **`artifacts/`**: Armazena artefatos gerados, geralmente em formato `.pkl`, como modelos treinados (`artifacts/models/`) e outros objetos de suporte (`artifacts/support/`).
- **`assets/`**: Arquivos estáticos como imagens, CSS e outros recursos utilizados pelo Dash.
- **`datasets/`**: Conjuntos de dados utilizados para análise e treinamento dos modelos.
- **`pages/`**: Contém os arquivos reponsáveis por gerar as páginas do Dash, sendo separados em páginas de formulários (`pages/forms/`) e relatórios de análises (`pages/reports/`).
- **`src/`**: Scripts utilitários, funções auxiliares e módulos de apoio ao projeto.
- **`main.py`**: Arquivo principal ("ponto único de entrada") para execução da aplicação Dash.
- **`requirements.txt`**: Lista de dependências do projeto.

```
dash-portifolio/
├── artifacts/
│   └── models/
│   └── support/
├── assets/
├── datasets/
├── pages/
│   ├── forms/
│   ├── reports/
├── src/
├── main.py
├── requirements.txt
```