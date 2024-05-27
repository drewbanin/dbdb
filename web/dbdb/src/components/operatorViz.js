import React, {useContext, useCallback, useState, useMemo} from 'react';
import ReactFlow, {
  ReactFlowProvider,
  Panel,
  MarkerType,
  useNodesState,
  useEdgesState,
  useReactFlow,

  getIncomers,
  getOutgoers,
} from 'reactflow';

import Dagre from 'dagre';

import { QueryContext } from '../Store.js';
import { OperatorNode } from "./OperatorNode.js"

const g = new Dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));

const getLayoutedElements = (nodes, edges, options) => {
  g.setGraph({
    rankdir: options.direction,
    nodesep: 100,
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

  const selectNode = (e, node) => {
      console.log("Selected node", node);

      const enumParents = (node) => {
          const parents = getIncomers(node, nodes, edges);

          let ret = [];
          parents.forEach(p => {
              const pp = enumParents(p);
              ret = ret.concat(pp);
          })

          return [node, ...ret]
      }

      const enumChildren = (node) => {
          const children = getOutgoers(node, nodes, edges);

          let ret = [];
          children.forEach(p => {
              const cc = enumChildren(p);
              ret = ret.concat(cc);
          })

          return [node, ...ret]
      }


      const parents = enumParents(node);
      const children = enumChildren(node);

      const toHighlight = parents.concat(children).map(n => n.id);

      const newEdges = edges.map(edge => {
          // if parent and child are in toHighlight, then highlight it!
          const lowlight = "#cccccc";
          const highlight = "#000000";

          if (toHighlight.indexOf(edge.source) >= 0 && toHighlight.indexOf(edge.target) >= 0) {
              edge.style = {color: highlight, stroke: highlight}
              edge.markerEnd = {...edge.markerEnd, color:  highlight};
              edge.data.highlight = true;
          } else {
              edge.style = {color: lowlight, stroke: lowlight}
              edge.markerEnd = {...edge.markerEnd, color: lowlight};
              edge.data.highlight = false;
          }

          return {...edge}
      })

      const lowEdges = newEdges.filter(e => !e.data.highlight);
      const highEdges = newEdges.filter(e => e.data.highlight);

      // Render highlighted edges after (above) non-highlighted edges
      setEdges([...lowEdges, ...highEdges]);
  }

  const nodeTypes = useMemo(() => ({
      operator: OperatorNode
  }), []);

  return (
    <ReactFlow
      nodes={nodes}
      edges={edges}
      onNodesChange={onNodesChange}
      onEdgesChange={onEdgesChange}
      nodeTypes={nodeTypes}
      onNodeClick={selectNode}
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
    const [ labels, setLabels ] = useState({nodes: {}})

    if (!nodeData || !nodeData.nodes) {
        return (<div style={{ marginTop: 10 }}>NO DATA</div>)
    }

    const RFNodes = nodeData.nodes.map((node) => {
        return {
            id: node.id + '',
            data: {
                label: node.name,
                id: node.id,
                details: node.details,
            },
            type: 'operator',
            position: { x: 0, y: 0 },
            sourcePosition: 'right',
            targetPosition: 'left',
            width: 200,
            height: 100
        }
    })

    let RFEdges = [];
    const nodeIds = RFNodes.map((node) => node.id);
    nodeIds.forEach((id) => {
        const edges = nodeData.edges[id];
        edges.forEach((edgeId) => {
            console.log("CREATING EDGE??")
            RFEdges.push({
                id: `${id}--${edgeId}`,
                source: edgeId + '',
                target: id + '',
                type: 'smoothstep',
                data: {},
                //label: "hi hi hi",
                sourceHandle: 'right',
                targetHandle: 'left',
                style: {
                    color: 'black'
                },
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
