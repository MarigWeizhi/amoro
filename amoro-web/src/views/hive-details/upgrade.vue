<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
/ -->

<script lang="ts" setup>
import { computed, onMounted, reactive, ref } from 'vue'
import { useRoute } from 'vue-router'
import schemaField from './components/Field.vue'
import partitionField from './components/Partition.vue'
import otherProperties from './components/Properties.vue'
import type { DetailColumnItem, IMap } from '@/types/common.type'
import { getHiveTableDetail, upgradeHiveTable } from '@/services/table.service'

const emit = defineEmits<{
  (e: 'goBack'): void
  (e: 'refresh'): void
}>()
const loading = ref<boolean>(false)
const field = reactive<DetailColumnItem[]>([])
const partitionFields = reactive<DetailColumnItem[]>([])
const propertiesObj = reactive<IMap<string>>({})
const pkName = reactive<IMap<string>[]>([])

const route = useRoute()

const params = computed(() => {
  return {
    ...route.query,
  }
})
const schemaFieldRef = ref()
const propertiesRef = ref()

async function getDetails() {
  try {
    const { catalog, db, table } = params.value
    if (!catalog || !db || !table) {
      return
    }
    loading.value = true
    partitionFields.length = 0
    field.length = 0
    const result = await getHiveTableDetail({
      ...params.value,
    })
    const { partitionColumnList = [], schema, properties } = result;
    (partitionColumnList || []).forEach((ele: DetailColumnItem) => {
      partitionFields.push(ele)
    });
    (schema || []).forEach((ele: DetailColumnItem) => {
      field.push(ele)
    })
    Object.assign(propertiesObj, properties)
  }
  catch (error) {
  }
  finally {
    loading.value = false
  }
}

function onConfirm() {
  getParams()
}

async function getParams() {
  pkName.length = 0
  const pkList = schemaFieldRef.value.getPkname()
  pkList.forEach((ele: DetailColumnItem) => pkName.push(ele as any))
  propertiesRef.value.getProperties().then((res: any) => {
    if (res) {
      Object.assign(propertiesObj, res)
      upgradeTable()
    }
  })
}

async function upgradeTable() {
  try {
    const { catalog, db, table } = params.value
    if (!catalog || !db || !table) {
      return
    }
    loading.value = true
    await upgradeHiveTable({
      ...params.value,
      pkList: pkName,
      properties: propertiesObj,
    })
    goBack()
    emit('refresh')
  }
  catch (error) {
    // failed
    goBack()
  }
  finally {
    loading.value = false
  }
}
function goBack() {
  emit('goBack')
}

function cancel() {
  goBack()
}

onMounted(() => {
  getDetails()
})
</script>

<template>
  <div class="upgrade-table">
    <div class="nav-bar">
      <left-outlined @click="goBack" />
      <span class="title g-ml-8">{{ $t('upgradeHiveTable') }}</span>
    </div>
    <div class="content">
      <div class="table-attrs">
        <a-form
          name="fields"
          class="label-120"
        >
          <a-form-item
            :label="$t('field')"
            name="field"
          >
            <schema-field ref="schemaFieldRef" :loading="loading" :fields="field" />
          </a-form-item>
          <a-form-item
            :label="$t('partitionField')"
            name="partitonField"
          >
            <partition-field :loading="loading" :partition-fields="partitionFields" />
          </a-form-item>
          <a-form-item
            :label="$t('otherProperties')"
            name="otherProperties"
          >
            <other-properties ref="propertiesRef" :properties-obj="propertiesObj" />
          </a-form-item>
        </a-form>
      </div>
      <div class="footer-btn">
        <a-button type="primary" :loading="loading" class="btn g-mr-12" @click="onConfirm">
          {{ $t('ok') }}
        </a-button>
        <a-button type="ghost" class="btn" @click="cancel">
          {{ $t('cancel') }}
        </a-button>
      </div>
    </div>
  </div>
</template>

<style lang="less" scoped>
.upgrade-table {
  height: 100%;
  display: flex;
  flex: 1;
  flex-direction: column;
  .nav-bar {
    padding-left: 12px;
    height: 20px;
    flex-shrink: 0;
  }
  .content {
    padding: 24px 24px 0;
    display: flex;
    flex: 1;
    flex-direction: column;
    width: 66%;
    justify-content: space-between;
    height: calc(100% - 32px);
    .table-attrs {
      display: flex;
      flex: 1;
      overflow-y: auto;
      padding-bottom: 48px;
    }
    .footer-btn {
      height: 32px;
      margin-top: 24px;
      .btn {
        min-width: 78px;
      }
    }
  }
}
</style>
