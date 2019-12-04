/**
 * @file transofrm.c
 * @author Mislav Novakovic <mislav.novakovic@sartur.hr>
 * @brief A bridge for connecting snabb and sysrepo data plane.
 *
 * @copyright
 * Copyright (C) 2019 Deutsche Telekom AG.
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

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <libyang/libyang.h>
#include <libyang/tree_data.h>
#include <libyang/tree_schema.h>
#include <sysrepo.h>
#include <sysrepo/values.h>
#include <sysrepo/xpath.h>

#include "common.h"
#include "libyang.h"
#include "snabb.h"
#include "transform.h"

#define COMMAND_MAX 256
#define BUFSIZE 256

/* transform xpath to snabb compatible format
 * 1) remove yang model from xpath
 * 2) remove "'" from the key value
 */
char *sr_xpath_to_snabb(char *xpath) {
  char *node = NULL;
  sr_xpath_ctx_t state = {0, 0, 0, 0};
  int rc = SR_ERR_OK;

  /* snabb xpath is always smaller than sysrepo's xpath */
  char *tmp = strdup(xpath);
  int len = strlen(xpath);

  CHECK_NULL_MSG(tmp, &rc, error, "failed to allocate memory");
  *tmp = '\0';  // init xpath string

  node = sr_xpath_next_node(xpath, &state);
  CHECK_NULL_MSG(node, &rc, error, "failed sr_xpath_next_node");

  while (true) {
    strncat(tmp, "/", len);
    if (NULL != node) {
      strncat(tmp, node, len);
    }

    while (true) {
      char *key, *value;
      /* iterate over key value pairs in xpath */
      key = sr_xpath_next_key_name(NULL, &state);
      if (NULL == key) {
        break;
      }
      strncat(tmp, "[", len);
      strncat(tmp, key, len);
      strncat(tmp, "=", len);
      value = sr_xpath_next_key_value(NULL, &state);
      strncat(tmp, value, len);
      strncat(tmp, "]", len);
    }
    /* iterate over nodes in xpath */
    node = sr_xpath_next_node(NULL, &state);

    if (NULL == node) {
      break;
    }
  }

error:
  sr_xpath_recover(&state);
  if (rc == SR_ERR_OK) {
    return tmp;
  } else {
    free(tmp);
    return NULL;
  }
}

/* transform xpath to snabb compatible format
 * 1) remove yang model from xpath
 * 2) remove "'" from the key value
 * 3) remove key's from the last node for add operation
 */
char *sr_xpath_to_snabb_no_end_keys(char *xpath) {
  char *node = NULL;
  sr_xpath_ctx_t state = {0, 0, 0, 0};
  int rc = SR_ERR_OK;

  /* snabb xpath is always smaller than sysrepo's xpath */
  char *tmp = strdup(xpath);
  int len = strlen(xpath);

  CHECK_NULL_MSG(tmp, &rc, error, "failed to allocate memory");
  *tmp = '\0';  // init xpath string

  node = sr_xpath_next_node(xpath, &state);
  CHECK_NULL_MSG(node, &rc, error, "failed sr_xpath_next_node");

  while (true) {
    strncat(tmp, "/", len);
    if (NULL != node) {
      strncat(tmp, node, len);
    }

    int current_pos = strlen(tmp);
    while (true) {
      char *key, *value;
      /* iterate over key value pairs in xpath */
      key = sr_xpath_next_key_name(NULL, &state);
      if (NULL == key) {
        break;
      }
      strncat(tmp, "[", len);
      strncat(tmp, key, len);
      strncat(tmp, "=", len);
      value = sr_xpath_next_key_value(NULL, &state);
      strncat(tmp, value, len);
      strncat(tmp, "]", len);
    }
    /* iterate over nodes in xpath */
    node = sr_xpath_next_node(NULL, &state);

    if (NULL == node) {
      /* remove keys if they exists in the last node
         when adding list's in snabb you can't have keys in the xpath */
      if (tmp[strlen(tmp) - 1] == ']') {
        tmp[current_pos] = '\0';
      }
      break;
    }
  }

error:
  sr_xpath_recover(&state);
  if (rc == SR_ERR_OK) {
    return tmp;
  } else {
    free(tmp);
    return NULL;
  }
}

void socket_close(global_ctx_t *ctx) {
  pthread_rwlock_wrlock(&ctx->snabb_lock);
  if (-1 != ctx->socket_fd) {
    close(ctx->socket_fd);
  }
  pthread_rwlock_unlock(&ctx->snabb_lock);
}

int socket_fetch(global_ctx_t *ctx, char *input, char **output) {
  int rc = SR_ERR_OK;
  int len = 0;
  int nbytes;
  char *buffer = NULL;

  if (NULL == input || !output) {
    return SR_ERR_INTERNAL;
  }

  /* get input length in char* format */
  char str[30];
  sprintf(str, "%d", (int)strlen(input));

  len = (int)strlen(&str[0]) + 1 + (int)strlen(input) + 1;

  buffer = malloc(sizeof(*buffer) * len);
  CHECK_NULL_MSG(buffer, &rc, error, "failed to allocate memory");

  *output = calloc(SNABB_SOCKET_MAX, sizeof(**output));
  CHECK_NULL_MSG(*output, &rc, error, "failed to allocate memory");

  nbytes = snprintf(buffer, len, "%s\n%s", str, input);

  pthread_rwlock_wrlock(&ctx->snabb_lock);
  nbytes = write(ctx->socket_fd, buffer, nbytes);
  if ((int)strlen(buffer) != (int)nbytes) {
    pthread_rwlock_unlock(&ctx->snabb_lock);
    ERR("Failed to write full input to server: written %d, expected %d",
        (int)nbytes, (int)strlen(buffer));
    rc = SR_ERR_INTERNAL;
    goto error;
  }
  read(ctx->socket_fd, *output, SNABB_SOCKET_MAX);
  pthread_rwlock_unlock(&ctx->snabb_lock);

  free(buffer);

  return rc;

error:
  if (input) {
    ERR("snabb input:\n%s", input);
  }
  if (strlen(*output)) {
    ERR("snabb output:\n%s", *output);
  }
  if (NULL != buffer) {
    free(buffer);
  }
  if (NULL != *output) {
    free(*output);
  }
  return SR_ERR_INTERNAL;
}

int socket_send(global_ctx_t *ctx, char *input, bool ignore_error) {
  int rc = SR_ERR_OK;
  int len = 0;
  int nbytes;
  char *buffer = NULL;
  // TODO add large char array
  char read_output[256] = {0};

  if (NULL == input) {
    return SR_ERR_INTERNAL;
  }

  /* get input length in char* format */
  char str[30];
  sprintf(str, "%d", (int)strlen(input));

  len = (int)strlen(&str[0]) + 1 + (int)strlen(input) + 1;

  buffer = malloc(sizeof(*buffer) * len);
  CHECK_NULL_MSG(buffer, &rc, error, "failed to allocate memory");

  nbytes = snprintf(buffer, len, "%s\n%s", str, input);

  // TODO add timeout check
  pthread_rwlock_wrlock(&ctx->snabb_lock);
  nbytes = write(ctx->socket_fd, buffer, nbytes);
  if ((int)strlen(buffer) != (int)nbytes) {
    pthread_rwlock_unlock(&ctx->snabb_lock);
    ERR("Failed to write full input to server: written %d, expected %d",
         (int)nbytes, (int)strlen(buffer));
    rc = SR_ERR_INTERNAL;
    goto error;
  }
  nbytes = read(ctx->socket_fd, read_output, 256);
  pthread_rwlock_unlock(&ctx->snabb_lock);

  free(buffer);
  buffer = NULL;

  read_output[nbytes] = 0;

  /* count new lines */
  int counter = 0;
  for (int i = 0; i < (int)strlen(read_output); i++) {
    if ('\n' == read_output[i]) {
      counter++;
    }
  }
  /* if it has 5 new lines that means it has 'status' parameter */
  if (0 == nbytes) {
    goto failed;
  } else if (5 == counter) {
    goto failed;
  } else if (21 != nbytes && 18 != nbytes) {
    goto failed;
  } else if (0 == nbytes) {
    goto failed;
  }

  /* set null terminated string at the beggining */
  read_output[0] = '\0';

  return rc;
failed:
  if (input) {
    ERR("snabb input:\n%s", input);
  }
  if (strlen(read_output)) {
    ERR("snabb output:\n%s", read_output);
  }
  if (ignore_error) {
    rc = SR_ERR_INTERNAL;
    WRN("Operation faild for:\n%s", input);
    WRN("Respons:\n%s", read_output);
  }
error:
  if (NULL != buffer) {
    free(buffer);
  }
  return SR_ERR_INTERNAL;
}

static sr_type_t sr_ly_data_type_to_sr(LY_DATA_TYPE type) {
  switch (type) {
    case LY_TYPE_BINARY:
      return SR_BINARY_T;
    case LY_TYPE_BITS:
      return SR_BITS_T;
    case LY_TYPE_BOOL:
      return SR_BOOL_T;
    case LY_TYPE_DEC64:
      return SR_DECIMAL64_T;
    case LY_TYPE_EMPTY:
      return SR_LEAF_EMPTY_T;
    case LY_TYPE_ENUM:
      return SR_ENUM_T;
    case LY_TYPE_IDENT:
      return SR_IDENTITYREF_T;
    case LY_TYPE_INST:
      return SR_INSTANCEID_T;
    case LY_TYPE_STRING:
      return SR_STRING_T;
    case LY_TYPE_INT8:
      return SR_INT8_T;
    case LY_TYPE_UINT8:
      return SR_UINT8_T;
    case LY_TYPE_INT16:
      return SR_INT16_T;
    case LY_TYPE_UINT16:
      return SR_UINT16_T;
    case LY_TYPE_INT32:
      return SR_INT32_T;
    case LY_TYPE_UINT32:
      return SR_UINT32_T;
    case LY_TYPE_INT64:
      return SR_INT64_T;
    case LY_TYPE_UINT64:
      return SR_UINT64_T;
    default:
      return SR_UNKNOWN_T;
      // LY_LEAF_REF
      // LY_DERIVED
      // LY_TYPE_UNION
  }
}

int libyang_to_sysrepo_value(sr_val_t *value, LY_DATA_TYPE type, lyd_val leaf) {
  int rc = SR_ERR_OK;
  /* try to build string data first */
  rc = sr_val_set_str_data(value, type, leaf.string);
  if (SR_ERR_OK == rc) {
    return rc;
  }

  value->type = sr_ly_data_type_to_sr(type);

  switch (type) {
    case LY_TYPE_BOOL:
      value->data.bool_val = leaf.bln;
      return SR_ERR_OK;
    case LY_TYPE_DEC64:
      value->data.decimal64_val = (double)leaf.dec64;
      return SR_ERR_OK;
    case LY_TYPE_UNION:
      return SR_ERR_OK;
    case LY_TYPE_INT8:
      value->data.int8_val = leaf.int8;
      return SR_ERR_OK;
    case LY_TYPE_UINT8:
      value->data.uint8_val = leaf.uint8;
      return SR_ERR_OK;
    case LY_TYPE_INT16:
      value->data.int16_val = leaf.int16;
      return SR_ERR_OK;
    case LY_TYPE_UINT16:
      value->data.uint16_val = leaf.uint16;
      return SR_ERR_OK;
    case LY_TYPE_INT32:
      value->data.int32_val = leaf.int32;
      return SR_ERR_OK;
    case LY_TYPE_UINT32:
      value->data.uint32_val = leaf.uint32;
      return SR_ERR_OK;
    case LY_TYPE_INT64:
      value->data.int64_val = leaf.int64;
      return SR_ERR_OK;
    case LY_TYPE_UINT64:
      value->data.uint64_val = leaf.uint64;
      return SR_ERR_OK;
    default:
      return SR_ERR_INTERNAL;
  }
}

int snabb_state_data_to_sysrepo(global_ctx_t *ctx, char *xpath,
                                sr_val_t **values, size_t *values_cnt) {
  int rc = SR_ERR_OK;
  // TODO calculate size
  char message[SNABB_MESSAGE_MAX] = {0};
  char *response = NULL;
  struct lyd_node *root = NULL;
  struct lyd_node *new_root = NULL;
  struct ly_set *root_set = NULL;
  int cnt = 0;

  CHECK_RET(rc, error, "failed to format xpath: %s", sr_strerror(rc));
  char *snabb_xpath = sr_xpath_to_snabb(xpath);
  snprintf(message, SNABB_MESSAGE_MAX, "get-state {path '%s'; schema %s;}",
           snabb_xpath, ctx->yang_model);
  free(snabb_xpath);

  /* send to socket */
  rc = socket_fetch(ctx, message, &response);
  CHECK_RET(rc, error, "failed to send message to snabb socket: %s",
            sr_strerror(rc));

  rc = transform_data_to_array(ctx, xpath, response, &root);
  CHECK_RET(rc, error, "failed parse snabb data in libyang: %s",
            sr_strerror(rc));

  /* move to root node based on xpath */
  root_set = lyd_find_path(root, xpath);
  CHECK_NULL_MSG(root_set, &rc, error, "failed lyd_find_path");
  if (root_set->number <= 0) {
    ERR("lyd_find_path did not find any data for xpath %s", xpath);
    goto error;
  }
  new_root = root_set->set.d[0];

  const struct lyd_node *node = NULL, *next = NULL;
  LY_TREE_DFS_BEGIN(new_root, next, node) {
    if (LYS_LEAF == node->schema->nodetype ||
        LYS_LEAFLIST == node->schema->nodetype) {
      cnt++;
    }
    LY_TREE_DFS_END(new_root, next, node);
  }

  sr_val_t *v = NULL;
  rc = sr_new_values(cnt, &v);
  CHECK_RET(rc, error, "failed sr_new_values: %s", sr_strerror(rc));

  int i = 0;
  LY_TREE_DFS_BEGIN(new_root, next, node) {
    if (LYS_LEAF == node->schema->nodetype ||
        LYS_LEAFLIST == node->schema->nodetype) {
      struct lyd_node_leaf_list *leaf = (struct lyd_node_leaf_list *)node;
      struct lys_node_leaf *lys_leaf = (struct lys_node_leaf *)node->schema;
      char *path = lyd_path(node);
      CHECK_NULL_MSG(path, &rc, error, "failed to allocate memory");

      rc = sr_val_set_xpath(&v[i], path);
      free(path);
      CHECK_RET(rc, error, "failed sr_val_set_xpath: %s", sr_strerror(rc));

      rc = libyang_to_sysrepo_value(&v[i], lys_leaf->type.base, leaf->value);
      CHECK_RET(rc, error, "failed to set value: %s", sr_strerror(rc));

      i++;
    }
    LY_TREE_DFS_END(new_root, next, node);
  }

  *values = v;
  *values_cnt = cnt;

error:
  /* free lyd_node */
  if (NULL != root_set) {
    ly_set_free(root_set);
  }
  if (NULL != root) {
    lyd_free(root);
  }
  if (NULL != response) {
    free(response);
  }
  return rc;
}

int libyang_data_to_sysrepo(sr_session_ctx_t *session, struct lyd_node *root) {
  const struct lyd_node *node = NULL, *next = NULL;
  char *xpath = NULL;
  int rc = SR_ERR_OK;

  LY_TREE_DFS_BEGIN(root, next, node) {
    if (LYS_LEAF == node->schema->nodetype ||
        LYS_LEAFLIST == node->schema->nodetype) {
      struct lyd_node_leaf_list *leaf = (struct lyd_node_leaf_list *)node;
      /* skip key nodes, sysrepo will show error messages in the logs */
      bool skip = false;
      if (LYS_LIST == node->parent->schema->nodetype) {
        struct lys_node_list *list =
            (struct lys_node_list *)node->parent->schema;
        for (int i = 0; i < list->keys_size; i++) {
          if (0 == strncmp(list->keys[i]->name, node->schema->name,
                           strlen(node->schema->name))) {
            skip = true;
          }
        }
      }
      if (!skip) {
        xpath = lyd_path(node);
        CHECK_NULL_MSG(xpath, &rc, error, "failed to allocate memory");

        rc = sr_set_item_str(session, xpath, leaf->value_str, NULL, SR_EDIT_DEFAULT);
        CHECK_RET(rc, error, "failed sr_set_item_str: %s", sr_strerror(rc));
        free(xpath);
        xpath = NULL;
      }
    }
    LY_TREE_DFS_END(root, next, node);
  }

  INF_MSG("apply the changes");
  rc = sr_apply_changes(session, 0);
  CHECK_RET(rc, error, "failed sr_apply_changes: %s", sr_strerror(rc));

  xpath = NULL;
error:
  if (NULL != xpath) {
    free(xpath);
  }
  return rc;
}

int snabb_datastore_to_sysrepo(global_ctx_t *ctx) {
  int rc = SR_ERR_OK;
  char message[SNABB_MESSAGE_MAX] = {0};
  char *response = NULL;
  struct lyd_node *node = NULL;

  snprintf(message, SNABB_MESSAGE_MAX,
           "get-config {path '/'; print-default 'true'; schema %s;}",
           ctx->yang_model);

  /* send to socket */
  INF_MSG("send to socket");
  rc = socket_fetch(ctx, message, &response);
  CHECK_RET(rc, error, "failed to send message to snabb socket: %s",
            sr_strerror(rc));

  rc = transform_data_to_array(ctx, NULL, response, &node);
  CHECK_RET(rc, error, "failed parse snabb data in libyang: %s",
            sr_strerror(rc));

  /* copy snabb daat to startup datastore */
  INF_MSG("appply snabb data to sysrepo startup datastore");
  rc = libyang_data_to_sysrepo(ctx->startup_sess, node);
  CHECK_RET(rc, error, "failed to apply libyang data to sysrepo: %s",
            sr_strerror(rc));

  /* free lyd_node */
  if (NULL != node) {
    lyd_free(node);
  }

error:
  if (NULL != response) {
    free(response);
  }
  return rc;
}

int sync_datastores(global_ctx_t *ctx) {
  char datastore_command[COMMAND_MAX] = {0};
  int rc = SR_ERR_OK;
  FILE *fp;

  /* check if the startup datastore is empty
   * by checking the output of sysrepocfg */

  snprintf(datastore_command, COMMAND_MAX, "sysrepocfg -X -d startup -m %s", ctx->yang_model);

  fp = popen(datastore_command, "r");
  CHECK_NULL_MSG(fp, &rc, cleanup, "popen failed");
  if (fgetc(fp) != EOF) {
    /* copy the sysrepo startup datastore to snabb */
    INF_MSG("copy sysrepo data to snabb");
    // rc = sysrepo_datastore_to_snabb(ctx);
    CHECK_RET(rc, cleanup, "failed to apply sysrepo startup data to snabb: %s",
              sr_strerror(rc));
  } else {
    /* copy the snabb datastore to sysrepo */
    INF_MSG("copy snabb data to sysrepo");
    rc = snabb_datastore_to_sysrepo(ctx);
    CHECK_RET(rc, cleanup, "failed to apply snabb data to sysrepo: %s",
              sr_strerror(rc));
  }

cleanup:
  if (fp) {
    fclose(fp);
  }

  return rc;
}

void clear_context(global_ctx_t *ctx) {
  /* free libyang context */
  if (!ctx) {
    return;
  }

  if (ctx->threads) {
    thpool_destroy(*ctx->threads);
  }

  /* free sysrepo subscription */
  if (ctx->sess && ctx->sub) {
    sr_unsubscribe(ctx->sub);
  }

  pthread_rwlock_destroy(&ctx->snabb_lock);
  pthread_rwlock_destroy(&ctx->iter_lock);

  /* clean config file context */
  if (ctx->cfg) {
    clean_cfg(ctx->cfg);
  }

  /* close snabb socket */
  socket_close(ctx);

  /* close startup session */
  if (ctx->startup_sess) {
    sr_session_stop(ctx->startup_sess);
  }

  /* close startup connection */
  if (ctx->startup_conn) {
    sr_disconnect(ctx->startup_conn);
  }
  /*
    if (ctx->libyang_ctx) {
      ly_ctx_destroy(ctx->libyang_ctx, NULL);
    }
  */
  /* free global context */
  INF("%s plugin cleanup finished.", ctx->yang_model);
  free(ctx);
}

int load_startup_datastore(global_ctx_t *ctx) {
  sr_conn_ctx_t *connection = NULL;
  sr_session_ctx_t *session = NULL;
  int rc = SR_ERR_OK;

  /* connect to sysrepo */
  rc = sr_connect(SR_CONN_DEFAULT, &connection);
  CHECK_RET(rc, cleanup, "Error by sr_connect: %s", sr_strerror(rc));

  /* start session */
  rc = sr_session_start(connection, SR_DS_STARTUP, &session);
  CHECK_RET(rc, cleanup, "Error by sr_session_start: %s", sr_strerror(rc));

  ctx->startup_conn = connection;
  ctx->startup_sess = session;

  return rc;
cleanup:
  if (NULL != session) {
    sr_session_stop(session);
  }
  if (NULL != connection) {
    sr_disconnect(connection);
  }

  return rc;
}

static int get_config_leader_pid(int32_t *pid) {
  int32_t ignore_pid = 0;
  int rc = SR_ERR_OK;
  char *ret = NULL;
  FILE *snabb_ps = NULL;
  char line[BUFSIZE];

  // extract pid from the command "snabb ps"
  snabb_ps = popen("snabb ps", "r");
  CHECK_NULL_MSG(snabb_ps, &rc, cleanup, "Error opening pipe");

  ret = fgets(line, sizeof(line), snabb_ps);
  CHECK_NULL_MSG(ret, &rc, cleanup, "First line of snabb ps is empty");
  ret = fgets(line, sizeof(line), snabb_ps);
  CHECK_NULL_MSG(ret, &rc, cleanup, "Second line of snabb ps is empty");

  if (sscanf(line, "  \\- %d   worker for %d", &ignore_pid, pid) != 2) {
    ERR_MSG("Error running 'snabb ps' command.");
    rc = SR_ERR_INTERNAL;
    goto cleanup;
  }

cleanup:
  if (snabb_ps) {
    fclose(snabb_ps);
  }

  return rc;
}

int send_config_listen_command(global_ctx_t *ctx, const char *socket_path, int32_t pid) {
  int rc = SR_ERR_OK;
  char config_listen_command[COMMAND_MAX] = {0};
  FILE *config_listen = NULL;

  snprintf(config_listen_command, COMMAND_MAX, "snabb config listen -c %s %d", socket_path, pid);

  config_listen = popen(config_listen_command, "r");
  CHECK_NULL_MSG(config_listen, &rc, cleanup, "Error opening pipe");
  
cleanup:
  // TODO: this definitely shouldn't be closed here, but through ctx, when the sysrepo plugin exit
  if (config_listen) {
    pclose(config_listen);
  }

  return rc;
}

int snabb_socket_connect(global_ctx_t *ctx) {
  struct sockaddr_un address;
  const char *socket_path = "/tmp/snabb-sysrepo-plugin.sock";
  int32_t pid = 0;
  int rc = SR_ERR_OK;

  rc = get_config_leader_pid(&pid);
  CHECK_RET_MSG(rc, cleanup, "failed to get snabb config leader pid");

  rc = send_config_listen_command(ctx, socket_path, pid);
  CHECK_RET_MSG(rc, cleanup, "failed to send snabb config listen command");

  ctx->socket_fd = socket(PF_UNIX, SOCK_STREAM, 0);
  if (ctx->socket_fd < 0) {
    WRN("failed to create UNIX socket: %d", ctx->socket_fd);
    rc = SR_ERR_INTERNAL;
    goto cleanup;
  }

  strncpy(ctx->socket_path, socket_path, UNIX_PATH_MAX);
  ctx->socket_path[UNIX_PATH_MAX - 1] = 0;

  /* start with a clean address structure */
  memset(&address, 0, sizeof(struct sockaddr_un));

  address.sun_family = AF_UNIX;
  strncpy(address.sun_path, socket_path, UNIX_PATH_MAX);
  address.sun_path[UNIX_PATH_MAX - 1] = 0;

  rc = connect(ctx->socket_fd, (struct sockaddr *)&address,
               sizeof(struct sockaddr_un));
  CHECK_RET_MSG(rc, cleanup, "failed connection to snabb socket");

cleanup:
  if (rc != SR_ERR_OK) {
    socket_close(ctx);
    rc = SR_ERR_INTERNAL;
  }

  return rc;
}

bool is_new_snabb_command(iter_change_t *iter, iter_change_t *prev) {
  /* edge case, can't add/remove on XPATH /softwire-config/instance only edit */
  char *cmp_xpath = "/snabb-softwire-v2:softwire-config/instance";
  sr_val_t *tmp_val =
      (SR_OP_DELETED == iter->oper) ? iter->old_val : iter->new_val;
  if (0 == strncmp(cmp_xpath, tmp_val->xpath, strlen(cmp_xpath))) {
    if (tmp_val->type >= SR_BINARY_T && iter->oper != SR_OP_DELETED) {
      iter->oper = SR_OP_MODIFIED;
      return true;
    }
    return false;
  }

  /* one snabb command for creating list or containers */
  // print_change(iter->oper, iter->old_val, iter->new_val);
  if (SR_OP_CREATED == iter->oper && iter->new_val->type == SR_LIST_T) {
    return true;
  }

  /* one snabb command for deleting list or containers
  lib/yang/path_data.lua:436: Remove only allowed on arrays and tables */
  if (SR_OP_DELETED == iter->oper && iter->old_val->type == SR_LIST_T) {
    return true;
  }

  /* one snabb command for changing leaf's */
  if (SR_OP_MODIFIED == iter->oper) {
    return true;
  }

  return false;
}

int sr_modified_operation(global_ctx_t *ctx, sr_val_t *val) {
  int rc = SR_ERR_OK;
  char *message = NULL;
  char *snabb_xpath = NULL;
  char *leaf = NULL;

  leaf = sr_val_to_str(val);
  CHECK_NULL_MSG(leaf, &rc, cleanup, "failed to allocate memory");
  snabb_xpath = sr_xpath_to_snabb(val->xpath);
  CHECK_NULL_MSG(snabb_xpath, &rc, cleanup, "failed to allocate memory");

  int len = 47 + strlen(snabb_xpath) + strlen(ctx->yang_model) + strlen(leaf);
  message = malloc(sizeof(*message) * len);
  CHECK_NULL_MSG(message, &rc, cleanup, "failed to allocate memory");

  snprintf(message, len, "set-config {path '%s'; config '%s'; schema %s;}",
           snabb_xpath, leaf, ctx->yang_model);

  /* send to socket */
  rc = socket_send(ctx, message, false);
  CHECK_RET(rc, cleanup, "failed to send message to snabb socket: %s",
            sr_strerror(rc));

cleanup:
  if (message) {
    free(message);
  }
  if (snabb_xpath) {
    free(snabb_xpath);
  }
  if (leaf) {
    free(leaf);
  }
  return rc;
}

int sr_deleted_operation(global_ctx_t *ctx, sr_val_t *val) {
  int rc = SR_ERR_OK;
  char *snabb_xpath = NULL;
  char *message = NULL;

  snabb_xpath = sr_xpath_to_snabb(val->xpath);
  CHECK_NULL_MSG(snabb_xpath, &rc, cleanup, "failed to allocate memory");

  int len = 38 + strlen(snabb_xpath) + strlen(ctx->yang_model);
  message = malloc(sizeof(*message) * len);
  CHECK_NULL_MSG(message, &rc, cleanup, "failed to allocate memory");

  snprintf(message, len, "remove-config {path '%s'; schema %s;}", snabb_xpath,
           ctx->yang_model);

  /* send to socket */
  rc = socket_send(ctx, message, false);
  CHECK_RET(rc, cleanup, "failed to send message to snabb socket: %s",
            sr_strerror(rc));

cleanup:
  if (message) {
    free(message);
  }
  if (snabb_xpath) {
    free(snabb_xpath);
  }
  return rc;
}

int double_message_size(char **message, int *len) {
  int rc = SR_ERR_OK;
  char *tmp = NULL;

  *len = *len * 2;
  tmp = (char *)realloc(*message, (*len));
  CHECK_NULL_MSG(tmp, &rc, error, "failed to allocate memory");

  *message = tmp;

error:
  return rc;
}

int lyd_to_snabb_json(struct lyd_node *node, char **message, int *len) {
  bool add_brackets = false;
  int rc = SR_ERR_OK;

  if (!len || !message || !*message) {
    return SR_ERR_INTERNAL;
  }

  if (**message == '\0') {
    add_brackets = true;
    strncat(*message, "{ ", *len);
  }

  while (node) {
    if (node->child && (node->schema->flags == LYS_CONTAINER ||
                        node->schema->flags == LYS_LIST ||
                        node->schema->flags == LYS_CHOICE)) {
      if (*len < 7 + (int)strlen(*message)) {
        rc = double_message_size(message, len);
        CHECK_RET(rc, error, "failed to double the buffer size: %s",
                  sr_strerror(rc));
      }
      strncat(*message, node->schema->name, *len);
      strncat(*message, " { ", *len);
      rc = lyd_to_snabb_json(node->child, message, len);
      CHECK_RET(rc, error, "failed lyd_to_snabb_json: %s", sr_strerror(rc));
      strncat(*message, " } ", *len);
    } else {
      struct lyd_node_leaf_list *leaf = (struct lyd_node_leaf_list *)node;
      if (*len < 4 + strlen(node->schema->name) + strlen(leaf->value_str) +
                     (int)strlen(*message)) {
        rc = double_message_size(message, len);
        CHECK_RET(rc, error, "failed to double the buffer size: %s",
                  sr_strerror(rc));
      }
      strncat(*message, node->schema->name, *len);
      strncat(*message, " ", *len);
      strncat(*message, leaf->value_str, *len);
      strncat(*message, "; ", *len);
    }
    node = node->next;
  }

  if (add_brackets) {
    strncat(*message, "}", *len);
  }

error:
  return rc;
}

int sr_created_operation(global_ctx_t *ctx, iter_change_t **p_iter,
                         size_t begin, size_t end) {
  iter_change_t *iter = *p_iter;
  char *snabb_xpath = NULL;
  char *message = NULL;
  char *data = NULL;
  int rc = SR_ERR_OK;
  struct lyd_node *root = NULL;
  char *xpath = NULL;

  pthread_rwlock_rdlock(&ctx->iter_lock);
  sr_val_t *create_val = iter[begin].new_val;
  xpath = iter[begin].new_val->xpath;
  for (size_t i = begin; i < end; ++i) {
    sr_val_t *val = iter[i].new_val;
    char *leaf = sr_val_to_str(val);
    if (root) {
      lyd_new_path(root, ctx->libyang_ctx, val->xpath, (void *)leaf, 0, 1);
    } else {
      root =
          lyd_new_path(NULL, ctx->libyang_ctx, val->xpath, (void *)leaf, 0, 1);
    }
    if (leaf) {
      free(leaf);
    }
  }
  pthread_rwlock_unlock(&ctx->iter_lock);

  struct ly_set *set = lyd_find_path(root, create_val->xpath);
  CHECK_NULL(set, &rc, cleanup, "failed lyd_find_path with path %s",
             create_val->xpath);

  data = malloc(sizeof(*data) * SNABB_MESSAGE_MAX);
  CHECK_NULL_MSG(data, &rc, cleanup, "failed to allocate memory");
  *data = '\0';
  int data_len = SNABB_MESSAGE_MAX;

  rc = lyd_to_snabb_json((*set->set.d)->child, &data, &data_len);
  CHECK_RET(rc, cleanup, "failed lyd_to_snabb_json: %s", sr_strerror(rc));

  // snabb xpath can't have leafs at the end
  snabb_xpath = sr_xpath_to_snabb_no_end_keys(xpath);
  CHECK_NULL_MSG(snabb_xpath, &rc, cleanup, "failed to allocate memory");

  int len = 47 + strlen(snabb_xpath) + strlen(data) + strlen(ctx->yang_model);
  message = malloc(sizeof(*message) * len);
  CHECK_NULL_MSG(message, &rc, cleanup, "failed to allocate memory");
  snprintf(message, len, "add-config {path '%s'; config '%s'; schema %s;}",
           snabb_xpath, data, ctx->yang_model);

  /* send to socket */
  rc = socket_send(ctx, message, false);
  CHECK_RET(rc, cleanup, "failed to send message to snabb socket: %s",
            sr_strerror(rc));

cleanup:
  if (message) {
    free(message);
  }
  if (data) {
    free(data);
  }
  if (snabb_xpath) {
    free(snabb_xpath);
  }
  if (set) {
    ly_set_free(set);
  }
  if (root) {
    lyd_free_withsiblings(root);
  }
  return rc;
}

void xpaths_to_snabb_socket(void *input) {
  int rc = SR_ERR_OK;
  thread_job_t *job = (thread_job_t *)input;
  CHECK_NULL_MSG(job, &rc, cleanup, "input is NULL");

  iter_change_t *iter = *job->p_iter;
  sr_change_oper_t oper;
  sr_val_t *tmp_val = NULL;

  pthread_rwlock_rdlock(&job->ctx->iter_lock);
  oper = iter[job->begin].oper;
  if (SR_OP_DELETED == oper) {
    tmp_val = iter[job->begin].old_val;
  } else if (SR_OP_MODIFIED == oper) {
    tmp_val = iter[job->begin].new_val;
  } else {
    tmp_val = iter[job->begin].new_val;
  }
  pthread_rwlock_unlock(&job->ctx->iter_lock);

  if (SR_OP_MODIFIED == oper) {
    rc = sr_modified_operation(job->ctx, tmp_val);
  } else if (SR_OP_DELETED == oper) {
    rc = sr_deleted_operation(job->ctx, tmp_val);
  } else {
    rc = sr_created_operation(job->ctx, job->p_iter, job->begin, job->end);
  }
  CHECK_RET(rc, cleanup, "failed to run operation: %s", sr_strerror(rc));

cleanup:
  if (rc != SR_ERR_OK) {
    *job->rc = rc;
  }
  if (job) {
    free(job);
  }
}
