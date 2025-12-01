# CineExplorer : Application CLI de Base de Données Cinématographique

Ce projet implémente une application en ligne de commande (CLI) pour explorer des données cinématographiques basées sur un sous-ensemble de l'IMDb.

## 🚀 Lancement de l'Application

1. **Cloner le répertoire.**
2. **Installer les dépendances :**
   ```bash
   python3 -m pip install -r requirements.txt
   ```
3. **Lancer l'application CLI :**
   ```bash
   python3 manage.py run
   ```

## 📂 Structure du Projet

- **data/** : Contient les fichiers CSV originaux et le notebook d'exploration (`exploration.ipynb`).
- **scripts/phase1_sqlite/** : Scripts de création de la base de données, requêtes SQL (Phase 1).
- **manage.py** : Point d'entrée de l'application.
- **cli.py** : Logique de l'interface utilisateur, gestion du menu et des saisies.
