
const LineDetail = ({ label, value }) => {
  return (
    <div className="lineDetail flexRow">
        <div className="lineDetail flexRowBox" style={{ flexGrow: 0 }}>{label}</div>
        <div className="LineDetail flexRowBox" style={{ flexGrow: 1 }}></div>
        <div className="lineDetail flexRowBox" style={{ flexGrow: 0, textAlign: "right" }}>{value}</div>
    </div>
  );
}

export { LineDetail };
