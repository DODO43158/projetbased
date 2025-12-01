"""Point d'entrée principal de l'application CineExplorer."""

import sys
from cli import run_cli 

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        print("Démarrage de l'application CineExplorer CLI...")
        run_cli()
    else:
        print("Usage: python3 manage.py run")
