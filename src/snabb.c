/**
 * @file snabb.c
 * @author Mislav Novakovic <mislav.novakovic@sartur.hr>
 * @brief Plugin for sysrepo datastore for management of snabb switch.
 *
 * @copyright
 * Copyright (C) 2017 Deutsche Telekom AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <stdio.h>
#include <syslog.h>

#include <pthread.h>
#include <sysrepo.h>
#include <sysrepo/values.h>

#include "cfg.h"
#include "common.h"
#include "config.h"
#include "libyang.h"
#include "snabb.h"
#include "thpool.h"
#include "transform.h"

const char *YANG_MODEL = YANG;

thread_job_t *create_job(global_ctx_t *ctx, iter_change_t **p_iter,
                         size_t begin, size_t end, int *rc) {
  thread_job_t *job = NULL;

  job = (thread_job_t *)malloc(sizeof(thread_job_t));
  if (job) {
    job->ctx = ctx;
    job->p_iter = p_iter;
    job->begin = begin;
    job->end = end;
    job->rc = rc;
  }

  return job;
}

static int parse_config(sr_session_ctx_t *session, const char *module_name,
                        global_ctx_t *ctx, sr_event_t event) {
  sr_change_iter_t *it = NULL;
  iter_change_t *iter_change = NULL;
  iter_change_t **p_iter_change = NULL;
  size_t iter_cnt = 0;
  size_t prev = 0;
  int rc = SR_ERR_OK;
  int thread_rc = SR_ERR_OK;
  sr_change_oper_t oper;
  sr_val_t *old_value = NULL;
  sr_val_t *new_value = NULL;
  iter_change = NULL;
  char xpath[XPATH_MAX_LEN] = {
      0,
  };

  /* create threads */
  threadpool thpool = thpool_init(THREADS);
  ctx->threads = &thpool;

  // initalize the array
  size_t iter_change_size = 10;
  iter_cnt = 0;
  iter_change = calloc(iter_change_size, sizeof(*iter_change));
  CHECK_NULL_MSG(iter_change, &rc, error, "failed to allocate memory");
  p_iter_change = &iter_change;

  snprintf(xpath, XPATH_MAX_LEN, "/%s:*//.", module_name);

  rc = sr_get_changes_iter(session, xpath, &it);
  if (SR_ERR_OK != rc) {
    ERR("Get changes iter failed for xpath %s", xpath);
    goto error;
  }

  INF_MSG("start iterating over the changes");
  bool skip = true;
  while (SR_ERR_OK ==
         sr_get_change_next(session, it, &oper, &old_value, &new_value)) {
    iter_change[iter_cnt].old_val = old_value;
    iter_change[iter_cnt].new_val = new_value;
    iter_change[iter_cnt].oper = oper;

    if (is_new_snabb_command(&iter_change[iter_cnt], &iter_change[prev])) {
      if (!skip) {
        thpool_add_work(
            thpool, xpaths_to_snabb_socket,
            create_job(ctx, p_iter_change, prev, iter_cnt, &thread_rc));
      }
      skip = false;
      prev = iter_cnt;
    }

    ++iter_cnt;

    /* lock the mutex and resize the array if needed */
    if (iter_cnt >= iter_change_size) {
      pthread_rwlock_wrlock(&ctx->iter_lock);
      iter_change_size *= 4;
      iter_change =
          realloc(iter_change, sizeof(*iter_change) * iter_change_size);
      p_iter_change = &iter_change;
      pthread_rwlock_unlock(&ctx->iter_lock);
      CHECK_NULL_MSG(iter_change, &rc, error, "failed to allocate memory");
    }
  }
  thpool_add_work(thpool, xpaths_to_snabb_socket,
                  create_job(ctx, p_iter_change, prev, iter_cnt, &thread_rc));

error:
  thpool_wait(thpool);
  thpool_destroy(thpool);
  ctx->threads = NULL;

  if (NULL != it) {
    sr_free_change_iter(it);
  }
  if (iter_change) {
    for (size_t i = 0; i < iter_cnt; ++i) {
      sr_free_val(iter_change[i].old_val);
      sr_free_val(iter_change[i].new_val);
    }
    free(iter_change);
    iter_change = NULL;
  }
  if (thread_rc != SR_ERR_OK) {
    return thread_rc;
  }
  return rc;
}

static int module_change_cb(sr_session_ctx_t *session, const char *module_name,
                            const char *xpath, sr_event_t event,
                            uint32_t request_id, void *private_data) {
  int rc = SR_ERR_OK;
  global_ctx_t *ctx = private_data;
  INF("%s configuration has changed.", ctx->yang_model);

  if (SR_EV_DONE == event) {
    if (false == ctx->cfg->sync_startup) {
      return SR_ERR_OK;
    }
    /* copy running datastore to startup */

    rc = sr_copy_config(ctx->startup_sess, module_name, SR_DS_RUNNING,
                        SR_DS_STARTUP, 0);
    if (SR_ERR_OK != rc) {
      WRN_MSG("Failed to copy running datastore to startup");
      /* TODO handle this error in snabb
       * there should be no errors at this stage of the process
       * */
      return rc;
    }
    return SR_ERR_OK;
  }

  rc = parse_config(session, module_name, ctx, event);
  CHECK_RET(rc, error, "failed to apply sysrepo changes to snabb: %s",
            sr_strerror(rc));

error:
  return rc;
}

static int state_data_cb(sr_session_ctx_t *session, const char *module_name,
                         const char *path, const char *request_xpath,
                         uint32_t request_id, struct lyd_node **parent,
                         void *private_data) {
  int rc = SR_ERR_OK;
  const struct ly_ctx *ly_ctx = NULL;
  char *value_string = NULL;
  sr_val_t *values = NULL;
  size_t values_cnt = 0;

  INF_MSG("state_data_cb");
  global_ctx_t *ctx = private_data;
  rc = snabb_state_data_to_sysrepo(ctx, (char *)path, &values, &values_cnt);
  CHECK_RET(rc, error, "failed to load state data: %s", sr_strerror(rc));

  if (*parent == NULL) {
    ly_ctx = sr_get_context(sr_session_get_connection(session));
    CHECK_NULL_MSG(ly_ctx, &rc, error,
                   "sr_get_context error: libyang context is NULL");
    *parent = lyd_new_path(NULL, ly_ctx, request_xpath, NULL, 0, 0);
  }

  for (size_t i = 0; i < values_cnt; i++) {
    value_string = sr_val_to_str(&values[i]);
    lyd_new_path(*parent, NULL, values[i].xpath, value_string, 0, 0);
    free(value_string);
    value_string = NULL;
  }

error:
  if (values != NULL) {
    sr_free_values(values, values_cnt);
    values = NULL;
    values_cnt = 0;
  }

  return rc;
}

int sr_plugin_init_cb(sr_session_ctx_t *session, void **private_ctx) {
  int rc = SR_ERR_OK;
  char xpath[XPATH_MAX_LEN] = {0};
  global_ctx_t *ctx = NULL;

  ctx = calloc(1, sizeof *ctx);
  CHECK_NULL_MSG(ctx, &rc, error, "failed malloc global context");

  ctx->yang_model = YANG_MODEL;
  ctx->sess = session;
  ctx->sub = NULL;
  ctx->socket_fd = -1;
  ctx->libyang_ctx = NULL;
  ctx->module = NULL;
  ctx->startup_conn = NULL;
  ctx->startup_sess = NULL;
  ctx->threads = NULL;
  ctx->cfg = NULL;

  /* init mutex for snabb socket */
  rc = pthread_rwlock_init(&ctx->snabb_lock, NULL);
  if (0 != rc) {
    rc = SR_ERR_INTERNAL;
    ERR_MSG("failed to create snabb rwlock");
    goto error;
  }

  /* init mutex for change_cb */
  rc = pthread_rwlock_init(&ctx->iter_lock, NULL);
  if (0 != rc) {
    ERR_MSG("failed to create rwlock");
    goto error;
  }

  /* get snabb socket */
  pthread_rwlock_wrlock(&ctx->snabb_lock);
  rc = snabb_socket_reconnect(ctx);
  CHECK_RET_MSG(rc, error, "failed to get socket from snabb");
  pthread_rwlock_unlock(&ctx->snabb_lock);

  /* set subscription as our private context */
  *private_ctx = ctx;

  /* parse the yang model */
  INF("Parse yang model %s with libyang", ctx->yang_model);
  rc = parse_yang_model(ctx);
  CHECK_RET(rc, error, "failed to parse yang model with libyang: %s",
            sr_strerror(rc));

  /* load the startup datastore */
  INF_MSG("load sysrepo startup datastore");
  rc = load_startup_datastore(ctx);
  CHECK_RET(rc, error, "failed to load startup datastore: %s", sr_strerror(rc));

  INF_MSG("sync sysrepo and snabb data");
  rc = sync_datastores(ctx);
  CHECK_RET(rc, error, "failed to apply sysrepo startup data to snabb: %s",
            sr_strerror(rc));

  rc = sr_copy_config(ctx->startup_sess, ctx->yang_model, SR_DS_STARTUP,
                      SR_DS_RUNNING, 0);
  if (SR_ERR_OK != rc) {
    WRN_MSG("Failed to copy running datastore to startup");
    /* TODO handle this error */
    goto error;
  }

  rc = sr_module_change_subscribe(ctx->sess, ctx->yang_model, NULL,
                                  module_change_cb, ctx, 0, SR_SUBSCR_DEFAULT,
                                  &ctx->sub);
  CHECK_RET(rc, error, "failed sr_module_change_subscribe: %s",
            sr_strerror(rc));

  snprintf(xpath, XPATH_MAX_LEN, "/%s:br-instances/binding/bind-instance/traffic-stat",
           ctx->yang_model);
  rc = sr_oper_get_items_subscribe(ctx->sess, ctx->yang_model, xpath,
                                   state_data_cb, ctx, SR_SUBSCR_CTX_REUSE,
                                   &ctx->sub);
  CHECK_RET(rc, error, "failed sr_dp_get_items_subscribe: %s", sr_strerror(rc));

  snprintf(xpath, XPATH_MAX_LEN, "/%s:br-instance/algorithm/algo-instance/traffic-stat", ctx->yang_model);
  rc = sr_oper_get_items_subscribe(ctx->sess, ctx->yang_model, xpath,
                                   state_data_cb, ctx, SR_SUBSCR_CTX_REUSE,
                                   &ctx->sub);
  CHECK_RET(rc, error, "failed sr_dp_get_items_subscribe: %s", sr_strerror(rc));

  /* load config file */
  ctx->cfg = init_cfg_file();
  CHECK_NULL_MSG(ctx->cfg, &rc, error, "failed to parse cfg config file");
  INF("%s plugin initialized successfully", ctx->yang_model);

error:
  return rc;
}

void sr_plugin_cleanup_cb(sr_session_ctx_t *session, void *private_ctx) {
  /* subscription was set as our private context */
  global_ctx_t *ctx = private_ctx;

  /* clean snabb related context */
  if (ctx) {
    clear_context(ctx);
  }
}

#ifndef PLUGIN
#include <signal.h>
#include <unistd.h>

volatile int exit_application = 0;

static void sigint_handler(__attribute__((unused)) int signum) {
  INF_MSG("Sigint called, exiting...");
  exit_application = 1;
}

int main() {
  INF_MSG("Plugin application mode initialized");
  sr_conn_ctx_t *connection = NULL;
  sr_session_ctx_t *session = NULL;
  void *private_ctx = NULL;
  int rc = SR_ERR_OK;

  ENABLE_LOGGING(SR_LL_DBG);

  /* connect to sysrepo */
  rc = sr_connect(SR_CONN_DEFAULT, &connection);
  CHECK_RET(rc, cleanup, "Error by sr_connect: %s", sr_strerror(rc));

  /* start session */
  rc = sr_session_start(connection, SR_DS_RUNNING, &session);
  CHECK_RET(rc, cleanup, "Error by sr_session_start: %s", sr_strerror(rc));

  rc = sr_plugin_init_cb(session, &private_ctx);
  CHECK_RET(rc, cleanup, "Error by sr_plugin_init_cb: %s", sr_strerror(rc));

  /* loop until ctrl-c is pressed / SIGINT is received */
  signal(SIGINT, sigint_handler);
  signal(SIGPIPE, SIG_IGN);
  while (!exit_application) {
    sleep(1); /* or do some more useful work... */
  }

cleanup:
  sr_plugin_cleanup_cb(session, private_ctx);

  sr_session_stop(session);
  sr_disconnect(connection);
}
#endif
