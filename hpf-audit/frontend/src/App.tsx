import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Layout, Menu } from 'antd'
import {
    MessageOutlined,
    ToolOutlined,
    DatabaseOutlined,
    DashboardOutlined
} from '@ant-design/icons'
import AgentPage from './pages/AgentPage'
import SkillsPage from './pages/SkillsPage'
import KnowledgePage from './pages/KnowledgePage'
import './App.css'

const { Header, Sider, Content } = Layout

function App() {
    return (
        <BrowserRouter>
            <Layout style={{ height: '100vh' }}>
                <Header style={{ padding: '0 24px', background: '#001529', color: 'white' }}>
                    <h2 style={{ margin: 0, color: 'white' }}>🏦 公积金智能审计系统</h2>
                </Header>
                <Layout>
                    <Sider width={200} style={{ background: '#fff' }}>
                        <Menu
                            mode="inline"
                            defaultSelectedKeys={['agent']}
                            style={{ height: '100%', borderRight: 0 }}
                            items={[
                                {
                                    key: 'agent',
                                    icon: <MessageOutlined />,
                                    label: <a href="/agent">Agent对话</a>
                                },
                                {
                                    key: 'skills',
                                    icon: <ToolOutlined />,
                                    label: <a href="/skills">Skills管理</a>
                                },
                                {
                                    key: 'knowledge',
                                    icon: <DatabaseOutlined />,
                                    label: <a href="/knowledge">知识库</a>
                                },
                                {
                                    key: 'dashboard',
                                    icon: <DashboardOutlined />,
                                    label: '仪表盘'
                                }
                            ]}
                        />
                    </Sider>
                    <Layout style={{ padding: '24px' }}>
                        <Content
                            style={{
                                background: '#fff',
                                padding: 24,
                                margin: 0,
                                minHeight: 280,
                                overflow: 'auto'
                            }}
                        >
                            <Routes>
                                <Route path="/" element={<Navigate to="/agent" replace />} />
                                <Route path="/agent" element={<AgentPage />} />
                                <Route path="/skills" element={<SkillsPage />} />
                                <Route path="/knowledge" element={<KnowledgePage />} />
                            </Routes>
                        </Content>
                    </Layout>
                </Layout>
            </Layout>
        </BrowserRouter>
    )
}

export default App
