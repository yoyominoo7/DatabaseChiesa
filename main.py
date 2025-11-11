from app import build_application

def main():
    app = build_application()
    app.run_polling()

if __name__ == "__main__":
    main()
