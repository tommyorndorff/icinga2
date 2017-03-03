/******************************************************************************
 * Icinga 2                                                                   *
 * Copyright (C) 2012-2017 Icinga Development Team (https://www.icinga.com/)  *
 *                                                                            *
 * This program is free software; you can redistribute it and/or              *
 * modify it under the terms of the GNU General Public License                *
 * as published by the Free Software Foundation; either version 2             *
 * of the License, or (at your option) any later version.                     *
 *                                                                            *
 * This program is distributed in the hope that it will be useful,            *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of             *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the              *
 * GNU General Public License for more details.                               *
 *                                                                            *
 * You should have received a copy of the GNU General Public License          *
 * along with this program; if not, write to the Free Software Foundation     *
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301, USA.             *
 ******************************************************************************/

#include "redis/rediswriter.hpp"
#include "redis/rediswriter.tcpp"
#include "remote/eventqueue.hpp"
#include "base/json.hpp"

using namespace icinga;

REGISTER_TYPE(RedisWriter);

/**
 * Starts the component.
 */
void RedisWriter::Start(bool runtimeCreated)
{
	ObjectImpl<RedisWriter>::Start(runtimeCreated);

	Log(LogInformation, "RedisWriter")
	    << "'" << GetName() << "' started.";

	m_ReconnectTimer = new Timer();
	m_ReconnectTimer->SetInterval(15);
	m_ReconnectTimer->OnTimerExpired.connect(boost::bind(&RedisWriter::TryToReconnect, this));
	m_ReconnectTimer->Start();
	m_ReconnectTimer->Reschedule(0);

	m_SubscriptionTimer = new Timer();
	m_SubscriptionTimer->SetInterval(15);
	m_SubscriptionTimer->OnTimerExpired.connect(boost::bind(&RedisWriter::UpdateSubscriptions, this));
	m_SubscriptionTimer->Start();
	m_SubscriptionTimer->Reschedule(0);

	boost::thread thread(boost::bind(&RedisWriter::HandleEvents, this));
	thread.detach();
}

void RedisWriter::ReconnectTimerHandler(void)
{
	m_WorkQueue.Enqueue(boost::bind(&RedisWriter::TryToReconnect, this));
}

void RedisWriter::TryToReconnect(void)
{
	if (m_Context)
		return;

	String path = GetPath();
	String host = GetHost();

	Log(LogInformation, "RedisWriter", "Trying to connect to redis server");

	if (path.IsEmpty())
		m_Context = redisConnect(host.CStr(), GetPort());
	else
		m_Context = redisConnectUnix(path.CStr());

	if (!m_Context || m_Context->err) {
		if (!m_Context) {
			Log(LogWarning, "RedisWriter", "Cannot allocate redis context.");
		} else {
			Log(LogWarning, "RedisWriter", "Connection error: ")
			    << m_Context->errstr;
		}

		if (m_Context) {
			redisFree(m_Context);
			m_Context = NULL;
		}

		return;
	}

	String password = GetPassword();

	if (!password.IsEmpty()) {
		redisReply *reply = reinterpret_cast<redisReply *>(redisCommand(m_Context, "AUTH %s", password.CStr()));

		if (!reply) {
			redisFree(m_Context);
			return;
		}

		if (reply->type == REDIS_REPLY_STATUS || reply->type == REDIS_REPLY_ERROR) {
			Log(LogInformation, "RedisWriter")
			    << "AUTH: " << reply->str;
		}

		freeReplyObject(reply);
	}
}

void RedisWriter::UpdateSubscriptionsTimerHandler(void)
{
	m_WorkQueue.Enqueue(boost::bind(&RedisWriter::UpdateSubscriptions, this));
}

void RedisWriter::UpdateSubscriptions(void)
{
	redisReply *reply = reinterpret_cast<redisReply *>(redisCommand(m_Context, "HGETALL icinga:subscription"));

	if (!reply) {
		redisFree(m_Context);
		return;
	}

	if (reply->type == REDIS_REPLY_STATUS || reply->type == REDIS_REPLY_ERROR) {
		Log(LogInformation, "RedisWriter")
		    << "AUTH: " << reply->str;
	}

	//TODO
	VERIFY(reply->type == REDIS_REPLY_ARRAY);

	for (int = 0; i < reply->elements; i++) {
		redisReply *reply1 = reply->element[i];

		//TODO
	}

	freeReplyObject(reply);
}

void RedisWriter::HandleEvents(void)
{
	String queueName = Utility::NewUniqueID();
	EventQueue::Ptr queue = new EventQueue(queueName);
	EventQueue::Register(queueName, queue);

	std::set<String> types;
	types.insert("CheckResult");
	types.insert("StateChange");
	types.insert("Notification");
	types.insert("AcknowledgementSet");
	types.insert("AcknowledgementCleared");
	types.insert("CommentAdded");
	types.insert("CommentRemoved");
	types.insert("DowntimeAdded");
	types.insert("DowntimeRemoved");
	types.insert("DowntimeStarted");
	types.insert("DowntimeTriggered");

	queue->SetTypes(types);

	queue->AddClient(this);

	for (;;) {
		Dictionary::Ptr event = queue->WaitForEvent(this);

		if (!event)
			continue;

		m_WorkQueue.Enqueue(boost::bind(&RedisWriter::HandleEvent, this, event));
	}

	queue->RemoveClient(this);
	EventQueue::UnregisterIfUnused(queueName, queue);
}

void RedisWriter::HandleEvent(const Dictionary::Ptr& event)
{
	redisReply *reply1 = reinterpret_cast<redisReply *>(redisCommand(m_Context, "INCR icinga:event.idx"));

	if (!reply1)
		return;

	if (reply1->type == REDIS_REPLY_STATUS || reply1->type == REDIS_REPLY_ERROR) {
		Log(LogInformation, "RedisWriter")
		    << "INCR icinga:event.idx: " << reply1->str;
	}

	if (reply1->type == REDIS_REPLY_ERROR) {
		freeReplyObject(reply1);
		return;
	}

	//TODO
	VERIFY(reply1->type == REDIS_REPLY_INTEGER);

	long long index = reply1->integer;

	freeReplyObject(reply1);

	String body = JsonEncode(event);

	//TODO: Verify that %lld is supported
	redisReply *reply2 = reinterpret_cast<redisReply *>(redisCommand(m_Context, "SET icinga:event.%lld %s", index, body.CStr()));

	if (!reply2)
		return;

	if (reply2->type == REDIS_REPLY_STATUS || reply2->type == REDIS_REPLY_ERROR) {
		Log(LogInformation, "RedisWriter")
		    << "SET icinga:event." << index << ": " << reply2->str;
	}

	if (reply2->type == REDIS_REPLY_ERROR) {
		freeReplyObject(reply2);
		return;
	}

	freeReplyObject(reply2);
}

void RedisWriter::Stop(bool runtimeRemoved)
{
	Log(LogInformation, "RedisWriter")
	    << "'" << GetName() << "' stopped.";

	ObjectImpl<RedisWriter>::Stop(runtimeRemoved);
}
