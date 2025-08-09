# Proyecto base flask

## Primeros comandos

```powershell
# 1. instalar la libreria
python -m pip install schema -t layer/python

# Para desplegar y configurar elk despliegue
#General
sam deploy --guided
#Personal
sam deploy --guided --profile aws-alisson-correo-outlook

# Para probar local
sam local invoke ./backend/fondos_gestionar --event ./backend/event.json
```

## Push - remoto

```powershell
git remote add <nombre-remoto> git@<profile>:<nombre-repo>.git
```
