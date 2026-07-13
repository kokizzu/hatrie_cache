import './styles.css';
import { mount } from 'svelte';
import Commands from './pages/Commands.svelte';

mount(Commands, {
  target: document.getElementById('app') as HTMLElement
});
