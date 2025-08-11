# Proyecto base flask

## Primeros comandos

```powershell
# 1. instalar la libreria
python -m pip install schema -t layer/python

# Para desplegar y configurar el despliegue inicialmente
# General
sam deploy --guided
# Personal
sam deploy --guided --profile aws-alisson-correo-outlook

# Para desplegar cambios
sam deploy

# Para probar local
python .\main.py
```

## Push - remoto

```powershell
git remote add <nombre-remoto> git@<profile>:<nombre-repo>.git
```
