1. Відкрити проєкт у Pycharm 
2. Встановити Flask та SQLAlchemy
3. Запустити через модуль if __name__ == "__main__", БД users.db буде створена автоматично з об'єктом Users та зазначеними властивостями
   - id Integer primary_key
   - username String(80) unique=True nullable=False
   - email = db.String(120) unique=True nullable=False
   - created_at = DateTime datetime.utcnow
4. Для додавання даних необхідно заповнювати лише username та email, всі інші заповнюються автоматично
5. Після запуску на основній сторінці за адресою http://127.0.0.1:8000/ відображається список та основний функціонал
    - Додавання запису
    - Видалення
    - Редагування