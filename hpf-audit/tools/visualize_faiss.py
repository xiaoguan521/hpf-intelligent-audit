"""
FAISS 向量库可视化工具
生成包含 Plotly 图表的静态 HTML 文件
"""
import os
import json
import numpy as np
from dotenv import load_dotenv
from typing import List, Dict, Any
from pathlib import Path

# 添加项目根目录到路径
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from hpf_audit.knowledge.vector_store import VectorStoreManager

# 加载环境变量
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))

# 获取项目根目录
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_INDEX_PATH = os.path.join(PROJECT_ROOT, "data", "faiss_index")

def generate_visualization(index_path=DEFAULT_INDEX_PATH, output_file="faiss_visualization.html"):
    print(f"🔄 Loading Vector Store from {index_path}...")
    try:
        vsm = VectorStoreManager(index_path=index_path)
        vs = vsm.vectorstore
    except Exception as e:
        print(f"❌ Failed to load vector store: {e}")
        return

    # Extract data
    print("🔄 Extracting data from FAISS index...")
    vectors = []
    metadata = []
    
    # Check if index_to_docstore_id exists (standard in LangChain FAISS)
    if not hasattr(vs, 'index_to_docstore_id'):
        print("❌ FAISS index structure not recognized (missing index_to_docstore_id).")
        return

    # Iterate through all indexed vectors
    # vs.index.ntotal gives total vectors
    total_vectors = vs.index.ntotal
    print(f"ℹ️ Found {total_vectors} vectors in index.")
    
    if total_vectors == 0:
        print("⚠️ Index is empty. Nothing to visualize.")
        return

    try:
        for i in range(total_vectors):
            # Retrieve vector
            vec = vs.index.reconstruct(i)
            vectors.append(vec)
            
            # Retrieve metadata
            doc_id = vs.index_to_docstore_id.get(i)
            doc_meta = {"id": f"unknown_{i}", "name": "Unknown", "description": "No metadata found"}
            if doc_id and doc_id in vs.docstore._dict:
                doc = vs.docstore.search(doc_id)
                # Filter init docs
                if doc.metadata.get("type") == "init":
                    doc_meta["name"] = "[System] Init Doc"
                    doc_meta["description"] = "Initialization document"
                    doc_meta["type"] = "init"
                else:
                    item_type = doc.metadata.get("type", "unknown")
                    # If knowledge type, use specific category for better grouping
                    if item_type == "knowledge":
                        item_type = doc.metadata.get("category", "knowledge")
                        
                    doc_meta = {
                        "id": doc.metadata.get("skill_id", doc_id),
                        "name": doc.metadata.get("name", "Unnamed") or doc.metadata.get("title", "Untitled"),
                        "description": doc.page_content[:200].replace("\n", " "),
                        "full_content": doc.page_content,
                        "type": item_type
                    }
            metadata.append(doc_meta)
            
    except Exception as e:
        print(f"❌ Error extraction data: {e}")
        return

    if not vectors:
        print("⚠️ No vectors extracted.")
        return

    # Dimensionality Reduction
    print("🔄 Running PCA (128d -> 2d)...")
    try:
        from sklearn.decomposition import PCA
        X = np.array(vectors)
        
        if len(vectors) < 2:
            print("⚠️ Not enough data for PCA (need >1), using random coordinates.")
            # Generate random 2D points for small data
            X_2d = np.random.rand(len(vectors), 2)
        else:
            pca = PCA(n_components=2)
            X_2d = pca.fit_transform(X)
        
        # Merge into plot data
        plot_data = []
        for i, coord in enumerate(X_2d):
            meta = metadata[i]
            plot_data.append({
                "x": float(coord[0]),
                "y": float(coord[1]),
                "name": meta["name"],
                "desc": meta["description"],
                "type": meta.get("type", "default")
            })
            
    except ImportError:
        print("❌ scikit-learn is required. Run: pip install scikit-learn")
        return
    except Exception as e:
        print(f"❌ PCA failed: {e}")
        return

    # Generate HTML
    print(f"🔄 Generating HTML to {output_file}...")
    
    html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>FAISS Vector Store Visualization</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        body {{ font-family: sans-serif; margin: 0; padding: 20px; background: #f0f2f6; }}
        .container {{ display: flex; height: 90vh; gap: 20px; }}
        .plot-area {{ flex: 2; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 10px; }}
        .info-area {{ flex: 1; background: white; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); padding: 20px; overflow-y: auto; }}
        h1 {{ margin-top: 0; color: #333; }}
        .card {{ border: 1px solid #eee; padding: 10px; margin-bottom: 10px; border-radius: 4px; }}
        .label {{ font-weight: bold; color: #666; font-size: 0.9em; }}
        #details-content {{ white-space: pre-wrap; }}
    </style>
</head>
<body>
    <h1>Skill Vector Visualization (PCA)</h1>
    <div class="container">
        <div class="plot-area" id="plot"></div>
        <div class="info-area">
            <h2>Details</h2>
            <div id="details">
                <p>Hover over or click a point to see details.</p>
            </div>
        </div>
    </div>

    <script>
        var data = {json.dumps(plot_data)};
        
        // Prepare Plotly traces
        var trace = {{
            x: data.map(d => d.x),
            y: data.map(d => d.y),
            text: data.map(d => d.name),
            mode: 'markers',
            marker: {{
                size: 12,
                color: data.map(d => {{
                    if (d.type === 'init') return '#ccc';
                    if (d.type === 'skill') return '#4CAF50';
                    if (d.type === 'regulation') return '#2196F3';
                    if (d.type === 'business_rule') return '#FF9800';
                    if (d.type === 'case_study') return '#9C27B0';
                    if (d.type === 'best_practice') return '#009688';
                    if (d.type === 'risk_rule') return '#F44336';
                    return '#607D8B'; // default
                }}),
                opacity: 0.8
            }},
            type: 'scatter',
            hoverinfo: 'text'
        }};

        var layout = {{
            title: 'Semantic Space',
            hovermode: 'closest',
            dragmode: 'pan',
            xaxis: {{ zeroline: false, showgrid: true }},
            yaxis: {{ zeroline: false, showgrid: true }}
        }};

        Plotly.newPlot('plot', [trace], layout, {{responsive: true}});

        var plotDiv = document.getElementById('plot');
        
        // Click event
        plotDiv.on('plotly_click', function(data){{
            var pt = data.points[0];
            var idx = pt.pointIndex;
            var item = window.data[idx];
            
            var html = `
                <div class="card">
                    <div class="label">Name</div>
                    <h3>${{item.name}}</h3>
                    <div class="label">Type</div>
                    <div>${{item.type}}</div>
                    <div class="label">Description</div>
                    <p>${{item.desc}}</p>
                </div>
            `;
            document.getElementById('details').innerHTML = html;
        }});
        
        // Hover event (optional, just name)
    </script>
</body>
</html>
    """
    
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_template)
        
    print(f"✅ Visualization saved to: {os.path.abspath(output_file)}")

if __name__ == "__main__":
    generate_visualization()
