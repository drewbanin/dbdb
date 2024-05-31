import React, { useContext } from 'react';
import { QueryContext } from '../Store.js';
import '../App.css';


function VolumePicker({ onVolumeChange }) {
    const { volume } = useContext(QueryContext);
    const [ musicVolume, setMusicVolume ] = volume;

    const onChangeVolume = function(volumeLevel) {
        setMusicVolume(volumeLevel);
        onVolumeChange(volumeLevel);
    }

    return (
        <div style={{
            height: 16,
            margin: 0,
            marginRight: 5,
            padding: "5px 3px 1px 3px",
            border: '1px solid black',
            display: 'inline-block'
        }}>
            <input
                className="volumeSlider"
                type="range"
                min="0"
                max="1"
                step="0.01"
                defaultValue={musicVolume}
                onChange={e => onChangeVolume(e.target.value)}

            />
        </div>
  );
}

export default VolumePicker;
