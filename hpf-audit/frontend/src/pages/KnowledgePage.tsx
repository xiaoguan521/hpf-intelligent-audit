import { useState, useEffect } from 'react'
import {
    Table,
    Card,
    Button,
    Input,
    Select,
    Tag,
    Space,
    Modal,
    Form,
    message,
    Popconfirm,
    Typography,
    Tooltip
} from 'antd'
import {
    PlusOutlined,
    SearchOutlined,
    DeleteOutlined,
    BookOutlined,
    ReloadOutlined
} from '@ant-design/icons'
import { knowledgeApi, KnowledgeItem } from '../api/client'

const { Title, Paragraph } = Typography
const { Option } = Select
const { TextArea } = Input

const KnowledgePage = () => {
    // State
    const [loading, setLoading] = useState(false)
    const [data, setData] = useState<KnowledgeItem[]>([])
    const [searchText, setSearchText] = useState('')
    const [categoryFilter, setCategoryFilter] = useState<string | undefined>(undefined)

    // Modal State
    const [isModalVisible, setIsModalVisible] = useState(false)
    const [submitLoading, setSubmitLoading] = useState(false)
    const [form] = Form.useForm()

    // Fetch Data
    const fetchData = async () => {
        setLoading(true)
        try {
            let res
            if (searchText) {
                res = await knowledgeApi.search(searchText)
            } else {
                res = await knowledgeApi.list({ category: categoryFilter, limit: 100 })
            }
            // Backend returns { status: 0, msg: "ok", data: { items: [], total: 0 } }
            if (res.data.status === 0) {
                setData(res.data.data.items)
            } else {
                message.error(res.data.msg || '加载失败')
            }
        } catch (error) {
            console.error(error)
            message.error('加载知识库失败，请检查后端服务')
        } finally {
            setLoading(false)
        }
    }

    // Initial Load & Filter Change
    useEffect(() => {
        fetchData()
    }, [categoryFilter])

    // Handle Search
    const handleSearch = () => {
        fetchData()
    }

    // Handle Add
    const handleAdd = async (values: any) => {
        setSubmitLoading(true)
        try {
            // Process tags: split string by comma if it's a string, or use as is
            const tags = values.tags ? values.tags.split(/[,，]/).map((t: string) => t.trim()).filter(Boolean) : []

            const payload = {
                ...values,
                tags: tags
            }

            const res = await knowledgeApi.add(payload)
            if (res.data.status === 0) {
                message.success('添加成功')
                setIsModalVisible(false)
                form.resetFields()
                fetchData() // Refresh list
            } else {
                message.error(res.data.msg || '添加失败')
            }
        } catch (error) {
            message.error('提交失败')
        } finally {
            setSubmitLoading(false)
        }
    }

    // Handle Delete
    const handleDelete = async (id: number) => {
        try {
            const res = await knowledgeApi.delete(id)
            if (res.data.status === 0) {
                message.success('删除成功')
                fetchData()
            } else {
                message.error(res.data.msg || '删除失败')
            }
        } catch (error) {
            message.error('删除请求失败')
        }
    }

    // Columns Configuration
    const columns = [
        {
            title: 'ID',
            dataIndex: 'id',
            width: 80,
            sorter: (a: KnowledgeItem, b: KnowledgeItem) => a.id - b.id,
        },
        {
            title: '类别',
            dataIndex: 'category',
            width: 120,
            render: (text: string) => {
                const colorMap: Record<string, string> = {
                    regulation: 'blue',
                    business_rule: 'orange',
                    case_study: 'purple',
                    best_practice: 'green',
                    risk_rule: 'red'
                }
                const labelMap: Record<string, string> = {
                    regulation: '法规政策',
                    business_rule: '业务规则',
                    case_study: '案例分析',
                    best_practice: '最佳实践',
                    risk_rule: '风险规则'
                }
                return <Tag color={colorMap[text] || 'default'}>{labelMap[text] || text}</Tag>
            }
        },
        {
            title: '标题',
            dataIndex: 'title',
            width: 250,
            render: (text: string) => <b>{text}</b>
        },
        {
            title: '标签',
            dataIndex: 'tags',
            width: 200,
            render: (text: string | string[]) => {
                if (!text) return '-'
                // Handle backend returning string "a,b" or actual array
                const tags = Array.isArray(text) ? text : String(text).split(',')
                return (
                    <Space size={[0, 4]} wrap>
                        {tags.map((tag, i) => (
                            tag && <Tag key={i}>{tag.trim()}</Tag>
                        ))}
                    </Space>
                )
            }
        },
        {
            title: '内容预览',
            dataIndex: 'content',
            ellipsis: true,
            render: (text: string) => (
                <Tooltip title={text} overlayStyle={{ maxWidth: 500 }}>
                    <span style={{ color: '#666' }}>
                        {text.substring(0, 50)}{text.length > 50 ? '...' : ''}
                    </span>
                </Tooltip>
            )
        },
        {
            title: '操作',
            key: 'action',
            width: 100,
            fixed: 'right' as const,
            render: (_: any, record: KnowledgeItem) => (
                <Popconfirm
                    title="确定要删除这条知识吗？"
                    description="删除后将同时从向量数据库中移除，不可恢复。"
                    onConfirm={() => handleDelete(record.id)}
                    okText="删除"
                    cancelText="取消"
                    okButtonProps={{ danger: true }}
                >
                    <Button type="text" danger icon={<DeleteOutlined />} />
                </Popconfirm>
            )
        }
    ]

    return (
        <div style={{ maxWidth: 1200, margin: '0 auto' }}>
            <div style={{ marginBottom: 24, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                    <Title level={2} style={{ marginBottom: 0 }}>
                        <BookOutlined /> 知识库管理
                    </Title>
                    <Paragraph type="secondary" style={{ marginBottom: 0 }}>
                        管理审计法规、业务规则和案例，为 AI 提供 RAG 知识检索支持
                    </Paragraph>
                </div>
                <Space>
                    <Button icon={<ReloadOutlined />} onClick={() => fetchData()}>刷新</Button>
                    <Button type="primary" icon={<PlusOutlined />} onClick={() => setIsModalVisible(true)}>
                        新增知识
                    </Button>
                </Space>
            </div>

            <Card style={{ marginBottom: 24 }}>
                <Space size="large">
                    <Space>
                        <span>分类筛选:</span>
                        <Select
                            style={{ width: 150 }}
                            placeholder="全部类型"
                            allowClear
                            onChange={setCategoryFilter}
                            value={categoryFilter}
                        >
                            <Option value="regulation">法规政策</Option>
                            <Option value="business_rule">业务规则</Option>
                            <Option value="case_study">案例分析</Option>
                            <Option value="best_practice">最佳实践</Option>
                            <Option value="risk_rule">风险规则</Option>
                        </Select>
                    </Space>
                    <Space>
                        <Input
                            placeholder="搜索标题或内容..."
                            prefix={<SearchOutlined />}
                            style={{ width: 300 }}
                            value={searchText}
                            onChange={(e) => setSearchText(e.target.value)}
                            onPressEnter={handleSearch}
                        />
                        <Button onClick={handleSearch}>搜索</Button>
                    </Space>
                </Space>
            </Card>

            <Table
                columns={columns}
                dataSource={data}
                rowKey="id"
                loading={loading}
                pagination={{
                    defaultPageSize: 10,
                    showSizeChanger: true,
                    showTotal: (total) => `共 ${total} 条`
                }}
            />

            {/* Create Modal */}
            <Modal
                title="新增知识条目"
                open={isModalVisible}
                onCancel={() => setIsModalVisible(false)}
                footer={null}
                width={700}
            >
                <Form
                    form={form}
                    layout="vertical"
                    onFinish={handleAdd}
                    initialValues={{ category: 'regulation' }}
                >
                    <Form.Item
                        name="title"
                        label="标题"
                        rules={[{ required: true, message: '请输入标题' }]}
                    >
                        <Input placeholder="例如：住房公积金提取管理办法" />
                    </Form.Item>

                    <Space style={{ display: 'flex', width: '100%' }} size="large">
                        <Form.Item
                            name="category"
                            label="分类"
                            rules={[{ required: true }]}
                            style={{ width: 200 }}
                        >
                            <Select>
                                <Option value="regulation">法规政策</Option>
                                <Option value="business_rule">业务规则</Option>
                                <Option value="case_study">案例分析</Option>
                                <Option value="best_practice">最佳实践</Option>
                                <Option value="risk_rule">风险规则</Option>
                            </Select>
                        </Form.Item>

                        <Form.Item
                            name="tags"
                            label="标签"
                            style={{ flex: 1 }}
                            help="多个标签用逗号分隔"
                        >
                            <Input placeholder="例如：提取, 退休, 政策" />
                        </Form.Item>
                    </Space>

                    <Form.Item
                        name="content"
                        label="详细内容"
                        rules={[{ required: true, message: '请输入内容' }]}
                    >
                        <TextArea
                            rows={10}
                            placeholder="输入法规全文或规则详情。AI 将使用此内容进行语义检索..."
                        />
                    </Form.Item>

                    <Form.Item style={{ textAlign: 'right', marginBottom: 0 }}>
                        <Space>
                            <Button onClick={() => setIsModalVisible(false)}>取消</Button>
                            <Button type="primary" htmlType="submit" loading={submitLoading}>
                                提交并索引
                            </Button>
                        </Space>
                    </Form.Item>
                </Form>
            </Modal>
        </div>
    )
}

export default KnowledgePage
