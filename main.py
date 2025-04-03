import asyncio
import logging
import os
from datetime import datetime
from signal import SIGINT, SIGTERM

import asyncpg
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, User
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
    """Инициализация схемы базы данных"""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                name TEXT NOT NULL,
                role TEXT NOT NULL,
                discipline TEXT
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS calls (
                id SERIAL PRIMARY KEY,
                judge_id BIGINT REFERENCES users(user_id),
                expert_id BIGINT REFERENCES users(user_id),
                discipline TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS hj_calls (
                id SERIAL PRIMARY KEY,
                judge_id BIGINT REFERENCES users(user_id) NOT NULL,
                head_judge_id BIGINT REFERENCES users(user_id),
                created_at TIMESTAMP NOT NULL,
                resolved_at TIMESTAMP)
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
    else:
        return await complete_registration(update, context)


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

        message = update.message or update.callback_query.message

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

        await message.reply_text("🎉 Регистрация завершена!")
        await show_main_menu(update, context)
        return ConversationHandler.END

    except Exception as e:
        logger.error(f"Ошибка при завершении регистрации: {str(e)}")
        message = update.callback_query.message if update.callback_query else update.message
        await message.reply_text("⚠️ Ошибка при сохранении данных. Попробуйте позже.")
        return ConversationHandler.END


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Отмена регистрации"""
    await update.message.reply_text("Регистрация отменена.")
    return ConversationHandler.END


async def call_expert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик вызова эксперта"""
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
                            [InlineKeyboardButton("Откликнуться", callback_data=f"respond_expert_{call_id}")]
                        ])
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки эксперту {expert['user_id']}: {e}")

        await query.edit_message_text("✅ Новый вызов эксперта создан!")
        await show_main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка при вызове эксперта: {e}")
        await query.edit_message_text("⚠️ Ошибка при вызове эксперта. Попробуйте позже.")


async def call_head_judge(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик вызова главного судьи с изменением сообщения"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    judge_id = query.from_user.id

    try:
        async with pool.acquire() as conn:
            judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1 AND role = 'judge'", judge_id)
            if not judge:
                await query.edit_message_text("❌ Только судьи могут вызывать главного судью!")
                return

            call_id = await conn.fetchval(
                """INSERT INTO hj_calls (judge_id, created_at) 
                VALUES ($1, $2) RETURNING id""",
                judge_id,
                datetime.now()
            )

            head_judges = await conn.fetch("SELECT user_id FROM users WHERE role = 'head_judge'")
            for hj in head_judges:
                try:
                    await context.bot.send_message(
                        hj['user_id'],
                        f"🔔 Судья {judge['name']} вызывает главного судью!\n"
                        f"📍 Дисциплина: {judge.get('discipline', 'не указана')}\n"
                        f"🆔 ID вызова: {call_id}",
                        reply_markup=InlineKeyboardMarkup([
                            [InlineKeyboardButton("Принять вызов", callback_data=f"respond_hj_{call_id}")]
                        ])
                    )
                except Exception as e:
                    logger.error(f"Ошибка отправки главному судье {hj['user_id']}: {e}")

        # Изменяем сообщение на "Запрос отправлен, ожидайте ответа"
        await query.edit_message_text(
            "✅ Запрос отправлен главным судьям. Ожидайте ответа...",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")]
            ])
        )

    except Exception as e:
        logger.error(f"Ошибка при вызове главного судьи: {e}")
        await query.edit_message_text("⚠️ Ошибка при вызове главного судьи. Попробуйте позже.")


async def respond_to_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Унифицированный обработчик отклика на вызов"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    responder_id = query.from_user.id
    call_type, call_id = query.data.split('_')[1], int(query.data.split('_')[2])

    try:
        async with pool.acquire() as conn:
            if call_type == "expert":
                # Обработка эксперта (оставляем как было)
                responder = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1 AND role = 'expert'", responder_id)
                if not responder:
                    await query.edit_message_text("❌ Только эксперты могут откликаться на этот вызов!")
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
                    responder_id,
                    call_id
                )

                judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", call['judge_id'])
                await context.bot.send_message(
                    judge['user_id'],
                    f"✅ Эксперт {responder['name']} ответил на ваш вызов!\n"
                    f"Он уже направляется к вам."
                )

                await query.edit_message_text("✅ Вы успешно откликнулись на вызов!")

            elif call_type == "hj":
                # Обработка главного судьи
                responder = await conn.fetchrow(
                    "SELECT * FROM users WHERE user_id = $1 AND role = 'head_judge'",
                    responder_id
                )
                if not responder:
                    await query.edit_message_text("❌ Только главные судьи могут принимать вызовы!")
                    return

                call = await conn.fetchrow(
                    """SELECT * FROM hj_calls 
                    WHERE id = $1 AND head_judge_id IS NULL
                    FOR UPDATE""",
                    call_id
                )
                if not call:
                    await query.edit_message_text("❌ Этот вызов уже обработан!")
                    return

                await conn.execute(
                    """UPDATE hj_calls 
                    SET head_judge_id = $1, resolved_at = $2
                    WHERE id = $3""",
                    responder_id,
                    datetime.now(),
                    call_id
                )

                judge = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", call['judge_id'])
                await context.bot.send_message(
                    judge['user_id'],
                    f"✅ Главный судья {responder['name']} принял ваш вызов!\n"
                    f"Он уже направляется к вам."
                )

                # Обновляем сообщение у главного судьи (оставляем его меню)
                await query.edit_message_text(
                    f"✅ Вы приняли вызов от судьи {judge['name']}\n"
                    f"Дисциплина: {judge.get('discipline', 'не указана')}"
                )

        # Обновляем меню для всех участников
        if call and 'judge_id' in call:
            # Обновляем меню судьи
            await show_main_menu(
                Update(
                    update.update_id,
                    callback_query=CallbackQuery(
                        id=str(update.update_id),
                        from_user=User(id=call['judge_id'], first_name=judge['name'], is_bot=False),
                        message=query.message,
                        chat_instance=query.chat_instance
                    )
                ),
                context
            )

        if call_type == "hj":
            # Обновляем меню главного судьи
            await show_main_menu(
                Update(
                    update.update_id,
                    callback_query=CallbackQuery(
                        id=str(update.update_id),
                        from_user=User(id=responder_id, first_name=responder['name'], is_bot=False),
                        message=query.message,
                        chat_instance=query.chat_instance
                    )
                ),
                context
            )

    except Exception as e:
        logger.error(f"Ошибка при отклике на вызов: {e}")
        await query.edit_message_text("⚠️ Ошибка при обработке вашего отклика.")



async def cancel_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик отмены вызовов (экспертов и главных судей)"""
    query = update.callback_query
    await query.answer()

    pool = context.bot_data['db_pool']
    judge_id = query.from_user.id

    try:
        async with pool.acquire() as conn:
            # Отменяем вызовы экспертов
            expert_calls = await conn.execute(
                "DELETE FROM calls WHERE judge_id = $1 AND expert_id IS NULL",
                judge_id
            )

            # Отменяем вызовы главного судьи
            hj_calls = await conn.execute(
                "DELETE FROM hj_calls WHERE judge_id = $1 AND head_judge_id IS NULL",
                judge_id
            )

            total_cancelled = int(expert_calls[-1]) + int(hj_calls[-1])
            if total_cancelled == 0:
                await query.edit_message_text("Нет активных вызовов для отмены.")
            else:
                await query.edit_message_text(f"✅ Отменено {total_cancelled} активных вызовов.")

        await show_main_menu(update, context)

    except Exception as e:
        logger.error(f"Ошибка при отмене вызова: {e}")
        await query.edit_message_text("⚠️ Ошибка при отмене вызова. Попробуйте позже.")


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает главное меню с информацией о статусе"""
    pool = context.bot_data['db_pool']
    user_id = update.effective_user.id

    try:
        message = update.message or update.callback_query.message

        async with pool.acquire() as conn:
            user = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)

            if not user:
                await message.reply_text("Пожалуйста, сначала зарегистрируйтесь через /start")
                return

            if user['role'] == "judge":
                active_expert_calls = await conn.fetchval(
                    "SELECT COUNT(*) FROM calls WHERE judge_id = $1 AND expert_id IS NULL",
                    user_id
                )

                active_hj_calls = await conn.fetchval(
                    "SELECT COUNT(*) FROM hj_calls WHERE judge_id = $1 AND head_judge_id IS NULL",
                    user_id
                )

                text = (
                    f"👨‍⚖️ Главное меню судьи ({user.get('discipline', 'без дисциплины')})\n"
                    f"Активных вызовов экспертов: {active_expert_calls}\n"
                    f"Активных вызовов главного судьи: {active_hj_calls}"
                )

                keyboard = [
                    [InlineKeyboardButton("📢 Вызвать эксперта", callback_data="call_expert")],
                    [InlineKeyboardButton("🆘 Вызвать главного судью", callback_data="call_head_judge")],
                    [InlineKeyboardButton("❌ Отменить все вызовы", callback_data="cancel_calls")],
                    [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")]
                ]

            elif user['role'] == "head_judge":
                active_calls = await conn.fetchval(
                    "SELECT COUNT(*) FROM hj_calls WHERE head_judge_id IS NULL"
                )

                text = (
                    "👨‍⚖️ Вы главный судья\n"
                    f"Ожидает обработки вызовов: {active_calls}"
                )

                keyboard = [
                    [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")]
                ]

            else:  # expert
                text = "🛎 Вы эксперт. Ожидайте вызовов."
                keyboard = [
                    [InlineKeyboardButton("🔄 Обновить статус", callback_data="refresh_status")]
                ]

        reply_markup = InlineKeyboardMarkup(keyboard)

        if update.callback_query:
            try:
                await update.callback_query.edit_message_text(
                    text,
                    reply_markup=reply_markup
                )
            except Exception as edit_error:
                if "Message is not modified" not in str(edit_error):
                    raise edit_error
        else:
            await message.reply_text(
                text,
                reply_markup=reply_markup
            )

    except Exception as e:
        logger.error(f"Ошибка в show_main_menu: {e}")
        message = update.message or (update.callback_query.message if update.callback_query else None)
        if message:
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

        application = Application.builder().token(os.getenv('BOT_TOKEN')).build()
        application.bot_data['db_pool'] = pool

        conv_handler = ConversationHandler(
            entry_points=[CommandHandler("start", start)],
            states={
                REGISTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, register_name)],
                REGISTER_ROLE: [CallbackQueryHandler(register_role, pattern="^(expert|judge|head_judge)$")],
                JUDGE_DISCIPLINE: [CallbackQueryHandler(judge_discipline,
                                                      pattern="^(relay|practicallego|linecons|BEAMline|narrowline|maze|robocup|airrace|sumo|aqua|onstage|walking|footballauto|rally|android|minisumo|microsumo)$")]
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            per_chat=True,
            per_user=True
        )

        application.add_handler(conv_handler)
        application.add_handler(CallbackQueryHandler(call_expert, pattern="^call_expert$"))
        application.add_handler(CallbackQueryHandler(call_head_judge, pattern="^call_head_judge$"))
        application.add_handler(CallbackQueryHandler(respond_to_call, pattern=r"^respond_(expert|hj)_\d+$"))
        application.add_handler(CallbackQueryHandler(cancel_call, pattern="^cancel_calls$"))
        application.add_handler(CallbackQueryHandler(refresh_status, pattern="^refresh_status$"))

        await application.initialize()
        await application.start()
        await application.updater.start_polling()

        loop = asyncio.get_running_loop()
        stop_event = asyncio.Event()

        for sig in (SIGINT, SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)

        await stop_event.wait()

    except Exception as e:
        print(e)
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