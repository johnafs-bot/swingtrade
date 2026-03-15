"""
SwingB3 — Entry point
Run: python run.py
"""
import sys
import os

# Ensure project root is in Python path
sys.path.insert(0, os.path.dirname(__file__))

from app import app

if __name__ == "__main__":
    print("=" * 60)
    print("  SwingB3 — Motor de Análise B3")
    print("  Acesse: http://localhost:5050")
    print("=" * 60)
    app.run(debug=False, host="0.0.0.0", port=5050)
