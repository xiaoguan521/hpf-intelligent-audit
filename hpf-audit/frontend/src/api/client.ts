import axios from 'axios'

const api = axios.create({
    baseURL: '/api',
    timeout: 30000
})

export interface ChatRequest {
    query: string
    use_langgraph?: boolean
    max_iterations?: number
}

export interface ChatResponse {
    answer: string
    reasoning_chain: Array<{
        iteration: number
        thought?: string
        type: string
        tool?: string
        result?: any
    }>
    iterations: number
    agent_type?: string
}

export interface Skill {
    skill_id: string
    name: string
    description: string
    template_type: string
    configuration: string
    is_active: number
    created_at?: string
    updated_at?: string
    config_parsed?: any
    tags?: string[]
}

export const agentApi = {
    chat: (data: ChatRequest) =>
        api.post<ChatResponse>('/agent/chat', data),

    listSkills: () =>
        api.get('/agent/skills'),

    generateSkill: (requirement: string, mode: string = 'shadow') =>
        api.post('/agent/generate_skill', { requirement, mode })
}

export const skillsApi = {
    list: (params?: any) =>
        api.get('/skills/list', { params }),

    detail: (skillId: string) =>
        api.get(`/skills/detail/${skillId}`),

    saveManual: (data: any) =>
        api.post('/skills/save_manual', data),

    update: (skillId: string, data: any) =>
        api.put(`/skills/update/${skillId}`, data),

    delete: (skillId: string) =>
        api.delete(`/skills/delete/${skillId}`),

    publish: (skillId: string) =>
        api.post(`/skills/publish/${skillId}`),

    unpublish: (skillId: string) =>
        api.post(`/skills/unpublish/${skillId}`),

    test: (skillId: string, params: any) =>
        api.post('/skills/test', { skill_id: skillId, params }),

    validateConfigFile: (config: any) =>
        api.post('/skills/validate', config)
}

export interface KnowledgeItem {
    id: number
    title: string
    category: string
    content: string
    tags?: string
    created_at?: string
}

export const knowledgeApi = {
    list: (params: { category?: string; limit?: number }) =>
        api.get('/knowledge/list', { params }),

    search: (q: string, limit: number = 10) =>
        api.get('/knowledge/search', { params: { q, limit } }),

    add: (data: { title: string; category: string; content: string; tags: string[] }) =>
        api.post('/knowledge/add', data),

    delete: (id: number) =>
        api.delete(`/knowledge/${id}`)
}

export default api
