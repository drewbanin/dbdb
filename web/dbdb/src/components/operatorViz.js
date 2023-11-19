import React, {useContext, useCallback, useEffect} from 'react';
import ReactFlow, {
  ReactFlowProvider,
  Panel,
  MarkerType,
  useNodesState,
  useEdgesState,
  useReactFlow,
} from 'reactflow';
import Dagre from 'dagre';

import { QueryContext } from '../Store.js';

const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));

const getLayoutedElements = (nodes, edges, options) => {
  g.setGraph({
    rankdir: options.direction,
    nodesep: 200,
    edgesep: 100,
    ranksep: 100,
  });

  edges.forEach((edge) => g.setEdge(edge.source, edge.target));
  nodes.forEach((node) => g.setNode(node.id, node));

  Dagre.layout(g);

  return {
    nodes: nodes.map((node) => {
      const { x, y } = g.node(node.id);

      return { ...node, position: { x, y } };
    }),
    edges,
  };
};

const LayoutFlow = ({RFNodes, RFEdges}) => {
  const { fitView, setCenter } = useReactFlow();
  const [nodes, setNodes, onNodesChange] = useNodesState(RFNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(RFEdges);

  const onLayout = useCallback(
    (direction) => {
      const layouted = getLayoutedElements(nodes, edges, { direction });

      setNodes([...layouted.nodes]);
      setEdges([...layouted.edges]);

      window.requestAnimationFrame(() => {
        fitView({
            padding: 0.3,
            includeHiddenNodes: true,
            nodes: layouted.nodes,
        });
      });
    },
    [nodes, edges]
  );


  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      fitView
      fitViewOptions={{ padding: 0.3, includeHiddenNodes: true, nodes: nodes }}
      proOptions={{ hideAttribution: true }}
    >
      <Panel position="top-right">
        <button onClick={() => onLayout('LR')}>RESET</button>
      </Panel>
    </ReactFlow>
  );
};


function OperatorViz(props) {
    const { query, result, nodes } = useContext(QueryContext);
    const [ nodeData, setNodeData ] = nodes;

    console.log("Node data", nodeData);

    if (!nodeData || !nodeData.nodes) {
        return (<div style={{ marginTop: 10 }}>NO DATA</div>)
    }
    
    const RFNodes = nodeData.nodes.map((node) => {
        return {
            id: node.id + '',
            data: {
                label: node.name,
            },
            type: 'default',
            width: 100,
            height: 20,
            position: { x: 0, y: 0 },
            sourcePosition: 'right',
            targetPosition: 'left',
        }
    })

    let RFEdges = [];
    const nodeIds = RFNodes.map((node) => node.id);
    nodeIds.forEach((id) => {
        const edges = nodeData.edges[id];
        edges.forEach((edgeId) => {
            RFEdges.push({
                id: `${id}--${edgeId}`,
                source: edgeId + '',
                target: id + '',
                type: 'smoothstep',
                data: {},
                //label: "hi hi hi",
                sourceHandle: 'right',
                targetHandle: 'left',
                style: {color: 'black'},
                markerEnd: {
                      type: MarkerType.ArrowClosed,
                      width: 20,
                      height: 20,
                      color: '#000000',
                },
            });
        });
    });


    const layouted = getLayoutedElements(RFNodes, RFEdges, {direction: "LR"});

    return (
        <div className="reactflowContainer">
            <ReactFlowProvider>
              <LayoutFlow RFNodes={[...layouted.nodes]} RFEdges={[...layouted.edges]} />
            </ReactFlowProvider>
        </div>
    );
}

export default OperatorViz;
