import asyncio
import logging
import os
from datetime import datetime
from signal import SIGINT, SIGTERM

import asyncpg
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes,
    ConversationHandler
)

load_dotenv()

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

REGISTER_NAME, REGISTER_ROLE, JUDGE_DISCIPLINE = range(3)


async def init_db():
    """Инициализация подключения к PostgreSQL"""
    return await asyncpg.create_pool(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        host=os.getenv('DB_HOST'),
        port=int(os.getenv('DB_PORT'))
    )


async def init_db_schema(pool):
    """Инициализация схемы базы данных без уникального ограничения"""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS calls (
                id SERIAL PRIMARY KEY,
                judge_id BIGINT REFERENCES users(user_id),
                expert_id BIGINT REFERENCES users(user_id),
                discipline TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
                /* Убрано UNIQUE (judge_id, expert_id) */
            )
        """)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработчик команды /start"""
    try:
        pool = context.bot_data['db_pool']
        user_id = update.effective_user.id

        async with pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

        if user:
            await (update.message or update.callback_query.message).reply_text("Вы уже зарегистрированы!")
            return ConversationHandler.END
        else:
            await update.message.reply_text("👋 Добро пожаловать! Введите ваше ФИО:")
            return REGISTER_NAME

    except Exception as e:
        logger.error(f"Ошибка в start: {str(e)}")
        await update.message.reply_text("⚠️ Ошибка при регистрации. Попробуйте позже.")
        return ConversationHandler.END


async def register_name(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка ввода ФИО"""
    context.user_data['name'] = update.message.text
    await update.message.reply_text(
        "Выберите вашу роль:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Эксперт", callback_data="expert")],
            [InlineKeyboardButton("Судья", callback_data="judge")],
            [InlineKeyboardButton("Главный судья", callback_data="head_judge")]
        ])
    )
    return REGISTER_ROLE


async def register_role(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка выбора роли"""
    query = update.callback_query
    await query.answer()

    role = query.data
    context.user_data['role'] = role

    if role == "judge":
        await query.edit_message_text(
            "Выберите вашу дисциплину:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Эстафета", callback_data="relay")],
                [InlineKeyboardButton("Практическая олимпиада LEGO", callback_data="practicallego")],
                [InlineKeyboardButton("Следование по линии обр. конструкторы", callback_data="linecons")],
                [InlineKeyboardButton("Следование по линии: BEAM", callback_data="BEAMline")],
                [InlineKeyboardButton("Следование по узкой линии", callback_data="narrowline")],
                [InlineKeyboardButton("Лабиринт", callback_data="maze")],
                [InlineKeyboardButton("RoboCup", callback_data="robocup")],
                [InlineKeyboardButton("Воздушные гонки", callback_data="airrace")],
                [InlineKeyboardButton("Сумо", callback_data="sumo")],
                [InlineKeyboardButton("Аквароботы", callback_data="aqua")],
                [InlineKeyboardButton("OnStage", callback_data="onstage")],
                [InlineKeyboardButton("Марафон шагающих роботов", callback_data="walking")],
                [InlineKeyboardButton("Футбол автономный", callback_data="footballauto")],
                [InlineKeyboardButton("Ралли по коридору", callback_data="rally")],
                [InlineKeyboardButton("Сумо андроидных роботов", callback_data="android")],
                [InlineKeyboardButton("Мини сумо", callback_data="minisumo")],
                [InlineKeyboardButton("Микро сумо", callback_data="microsumo")],

            ])
        )
        return JUDGE_DISCIPLINE
    elif role == "head_judge":
        return await complete_registration(update, context)
    else:
        return await complete_registration(update, context)


async def call_head_judge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик вызова главного судьи"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    user_id = query.from_user.id

    try:
        async with pool.acquire() as conn:
            judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if not judge:
                await query.edit_message_text("❌ Только судьи могут вызывать главного судью!")
                return

            head_judges = await conn.fetch("SELECT user_id FROM users WHERE role = 'head_judge'")

            if not head_judges:
                await query.edit_message_text("❌ Нет доступных главных судей!")
                return

            for head_judge in head_judges:
                try:
                    await context.bot.send_message(
                        head_judge['user_id'],
                        f"🔔 Судья {judge['name']} ({judge.get('discipline', '')}) вызывает главного судью!\n"
                        f"📍 Место: {judge.get('discipline', 'не указано')}\n"
                        f"👤 ID судьи: {user_id}",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Принять вызов", callback_data=f"respond_head_{user_id}")]
                        ])
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки главному судье {head_judge['user_id']}: {e}")

        await query.edit_message_text("✅ Запрос отправлен главному судье!")

    except Exception as e:
        logger.error(f"Ошибка при вызове главного судьи: {e}")
        await query.edit_message_text("⚠️ Ошибка при вызове главного судьи. Попробуйте позже.")


async def respond_head_judge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отклика главного судьи"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    head_judge_id = query.from_user.id
    judge_id = int(query.data.split('_')[2])  # Формат: respond_head_123456789

    try:
        async with pool.acquire() as conn:
            judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", judge_id)
            head_judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", head_judge_id)

            if not judge or not head_judge:
                await query.edit_message_text("❌ Ошибка: пользователь не найден!")
                return

            await context.bot.send_message(
                judge_id,
                f"✅ Главный судья {head_judge['name']} ответил на ваш вызов!\n"
                f"Он уже направляется к вам."
            )

        await query.edit_message_text(f"✅ Вы приняли вызов от судьи {judge['name']}")

    except Exception as e:
        logger.error(f"Ошибка при отклике главного судьи: {e}")
        await query.edit_message_text("⚠️ Ошибка при обработке вызова. Попробуйте позже.")

async def judge_discipline(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Обработка выбора дисциплины"""
    query = update.callback_query
    await query.answer()

    context.user_data['discipline'] = query.data
    return await complete_registration(update, context)


async def complete_registration(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Завершение регистрации и показ главного меню"""
    try:
        pool = context.bot_data['db_pool']
        user_data = context.user_data

        async with pool.acquire() as conn:
            if user_data['role'] == "judge":
                await conn.execute(
                    """INSERT INTO users (user_id, name, role, discipline)
                    VALUES ($1, $2, $3, $4)""",
                    update.effective_user.id,
                    user_data['name'],
                    user_data['role'],
                    user_data.get('discipline')
                )
            else:
                await conn.execute(
                    """INSERT INTO users (user_id, name, role)
                    VALUES ($1, $2, $3)""",
                    update.effective_user.id,
                    user_data['name'],
                    user_data['role']
                )

        await (update.callback_query.message if update.callback_query else update.message).reply_text(
            "🎉 Регистрация завершена!"
        )
        message = update.callback_query.message if update.callback_query else update.message
        await message.reply_text("🎉 Регистрация завершена!")

        await show_main_menu(update, context)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка при завершении регистрации: {str(e)}")
        await update.message.reply_text("⚠️ Ошибка при сохранении данных. Попробуйте позже.")
        return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена регистрации"""
    await update.message.reply_text("Регистрация отменена.")
    return ConversationHandler.END


async def call_expert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик вызова эксперта с возможностью нескольких вызовов"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    judge_id = query.from_user.id

    try:
        async with pool.acquire() as conn:
            judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1 AND role = 'judge'", judge_id)
            if not judge:
                await query.edit_message_text("❌ Только судьи могут вызывать экспертов!")
                return
            call_id = await conn.fetchval(
                """INSERT INTO calls (judge_id, discipline, created_at) 
                VALUES ($1, $2, $3) RETURNING id""",
                judge_id,
                judge['discipline'],
                datetime.now()
            )
            experts = await conn.fetch("SELECT user_id FROM users WHERE role = 'expert'")
            for expert in experts:
                try:
                    await context.bot.send_message(
                        expert['user_id'],
                        f"🔔 Судья {judge['name']} вызывает эксперта!\n"
                        f"📍 Дисциплина: {judge['discipline']}\n"
                        f"🆔 ID вызова: {call_id}",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Откликнуться", callback_data=f"respond_{call_id}")]
                        ])
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки эксперту {expert['user_id']}: {e}")

        await query.edit_message_text("✅ Новый вызов эксперта создан!")
        await show_main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка при вызове эксперта: {e}")
        await query.edit_message_text("⚠️ Ошибка при вызове эксперта. Попробуйте позже.")


async def respond_to_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отклика эксперта без проверки уникальности"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    expert_id = query.from_user.id
    call_id = int(query.data.split('_')[1])

    try:
        async with pool.acquire() as conn:
            expert = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1 AND role = 'expert'", expert_id)
            if not expert:
                await query.edit_message_text("❌ Только эксперты могут откликаться!")
                return

            call = await conn.fetchrow("SELECT * FROM calls WHERE id = $1", call_id)
            if not call:
                await query.edit_message_text("❌ Этот вызов не существует!")
                return

            if call['expert_id'] is not None:
                await query.edit_message_text("❌ Этот вызов уже занят другим экспертом!")
                return

            await conn.execute(
                "UPDATE calls SET expert_id = $1 WHERE id = $2",
                expert_id,
                call_id
            )

            judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", call['judge_id'])
            await context.bot.send_message(
                judge['user_id'],
                f"✅ Эксперт {expert['name']} ответил на ваш вызов!\n"
                f"Он уже направляется к вам."
            )

        await query.edit_message_text("✅ Вы успешно откликнулись на вызов!")

    except Exception as e:
        logger.error(f"Ошибка при отклике на вызов: {e}")
        await query.edit_message_text("⚠️ Ошибка при обработке вашего отклика.")

    except Exception as e:
        logger.error(f"Ошибка при отклике на вызов: {e}")
        await query.edit_message_text("⚠️ Ошибка при обработке вашего отклика.")


async def cancel_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отмены вызова эксперта"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    judge_id = query.from_user.id

    try:
        async with pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM calls WHERE judge_id = $1 AND expert_id IS NULL",
                judge_id
            )

            if result[-1] == '0':
                await query.edit_message_text("Нет активных вызовов для отмены.")
            else:
                await query.edit_message_text("✅ Все активные вызовы отменены.")
        await show_main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка при отмене вызова: {e}")
        await query.edit_message_text("⚠️ Ошибка при отмене вызова. Попробуйте позже.")


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает главное меню с постоянной кнопкой вызова"""
    pool = context.bot_data['db_pool']
    user_id = update.effective_user.id
    message = update.message or update.callback_query.message

    try:
        async with pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

        if not user:
            await message.reply_text("Пожалуйста, сначала зарегистрируйтесь через /start")
            return

        if user['role'] == "judge":
            keyboard = [
                [InlineKeyboardButton("📢 Вызвать эксперта", callback_data="call_expert")],
                [InlineKeyboardButton("🆘 Вызвать главного судью", callback_data="call_head_judge")],
                [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            async with pool.acquire() as conn:
                active_call = await conn.fetchrow(
                    "SELECT * FROM calls WHERE judge_id = $1 AND expert_id IS NULL",
                    user_id
                )

            status_text = "❌ Нет активных вызовов" if not active_call else \
                f"🟡 Ожидаем эксперта (ID: {active_call['id']})"
            if update.callback_query:
                await update.callback_query.edit_message_text(
                    f"Главное меню судьи ({user.get('discipline', '')})\n"
                    f"Текущий статус: {status_text}",
                    reply_markup=reply_markup
                )
            else:
                await message.reply_text(
                    f"Главное меню судьи ({user.get('discipline', '')})\n"
                    f"Текущий статус: {status_text}",
                    reply_markup=reply_markup
                )
        elif user['role'] == "head_judge":
            await message.reply_text(
                "👨‍⚖️ Вы главный судья. Ожидайте вызовов от других судей.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")]
                ])
            )
        else:
            await message.reply_text("🛎 Вы эксперт. Ожидайте вызовов.")

    except Exception as e:
        logger.error(f"Ошибка в show_main_menu: {e}")
        await message.reply_text("⚠️ Произошла ошибка. Попробуйте позже.")


async def refresh_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновляет статус вызовов"""
    query = update.callback_query
    await query.answer("Статус обновлен")
    await show_main_menu(update, context)


async def main():
    """Основная функция"""
    pool = None
    application = None

    try:
        pool = await init_db()
        await init_db_schema(pool)

        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
              user_id BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    role TEXT NOT NULL,
                    discipline TEXT
                )
            """)

        application = Application.builder().token(os.getenv('BOT_TOKEN')).build()
        application.bot_data['db_pool'] = pool

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                REGISTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_name)],
                REGISTER_ROLE: [CallbackQueryHandler(register_role, pattern="^(expert|judge)$")],
                JUDGE_DISCIPLINE: [CallbackQueryHandler(judge_discipline,
                                                        pattern="^(relay|practicallego|linecons|BEAMline|narrowline|maze|robocup|airrace|sumo|aqua|onstage|walking|footballauto|rally|android|minisumo|microsumo)$")]
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            per_chat=True,
            per_user=True
        )

        application.add_handler(conv_handler)

        await application.initialize()
        await application.start()
        await application.updater.start_polling()
        application.add_handler(CallbackQueryHandler(call_expert, pattern="^call_expert$"))
        application.add_handler(CallbackQueryHandler(respond_to_call, pattern=r"^respond_\d+$"))
        application.add_handler(CallbackQueryHandler(respond_to_call, pattern=r"^respond_\d+$"))
        application.add_handler(CallbackQueryHandler(refresh_status, pattern="^refresh_status$"))
        application.add_handler(CallbackQueryHandler(call_head_judge, pattern="^call_head_judge$"))
        application.add_handler(CallbackQueryHandler(respond_head_judge, pattern=r"^respond_head_\d+$"))

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        for sig in (SIGINT, SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        await stop_event.wait()

    except Exception as e:
        logger.error(f"Фатальная ошибка: {str(e)}")
    finally:
        if application:
            if application.updater and application.updater.running:
                await application.updater.stop()
            await application.stop()
            await application.shutdown()
        if pool:
            await pool.close()
        logger.info("Бот остановлен")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
