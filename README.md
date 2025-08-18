# Bem vindo(a) ao OpenRadio!

O OpenRadio é um script automático, que realiza uma transmissão em tempo real via RTMP para qualquer servidor, uma rádio automática que consegue através de uma query, baixar, adicionar overlays, e transmitir aquela música.

# Como funciona?

Simples, ele ao iniciar, pede o servidor RTMP ao qual ele deve transmitir, e uma query (nome da banda, ou estilo, ou enfim, tipo uma pesquisa google) e o resto, ele cuida pra você. Quando você inicia, ele realiza um scraping do SoundCloud, e realiza o download daquela música através do yt-dlp. Depois, ele apenas transmite.

# Como usar?
## Requisitos
Para começar, vocẽ precisa de:

- Um servidor Linux (Ubuntu 22.04+)
- Python3 e PIP instalados
- Sistema de pacotes atualizados
- Concordar, que ao executar esse script você se responsabiliza pelo uso.

## Aviso
Eu não me responsabilizo pelos problemas legais, ao baixar, você concorda que está baixando apenas para fins de estudo, e curiosidade pessoal.

## Passo a passo

1°: Realize o download do yt-dlp
```
sudo curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp
```

```
sudo chmod a+rx /usr/local/bin/yt-dlp
```

2° Realize o download do ffmpeg
```
sudo apt clean
```

```
sudo apt update
```

```
sudo apt install ffmpeg -y
```

3° Instale as dependencias do script
```
pip install requests beautifulsoup4 --break-system-packages
```

4° Baixe o script e execute!

## Exemplo de uso

- Configurações (Linhas: de 145 a 234)

```
python3 index.py
```


# Créditos
Criado por [op3n](https://github.com/op3ny)
