/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _NC_BACKEND_H_
#define _NC_BACKEND_H_

#include <nc_core.h>

typedef bool (*msg_backend_t)(struct context *ctx, struct conn *c_conn, struct msg* msg);
typedef bool (*msg_backend_parser_t)(struct context *ctx, struct conn *c_conn, struct msg* msg);

bool backend_process(struct context *ctx, struct conn *s_conn, struct msg* msg);
struct server* get_next_backend_server(struct msg* msg, struct conn* c_conn, uint8_t* key, uint32_t keylen);
bool backend_resend_q_empty(struct msg* msg);

#endif
